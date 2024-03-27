"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import socket
import threading
import queue
import shutil
import hashlib
from pathlib import Path


# Configure logging
LOGGER = logging.getLogger(__name__)

# 1. should I create another queue for reassign tasks?
# 2. 如果send shutdown message 遇到了connectionerror 怎么办？ mark as dead吗？

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        self.host = host
        self.port = port
        self.signals = {"shutdown": False}
        self.workers = {}
        self.job_queue = queue.Queue()
        self.job_count = 0
        self.finished_job_tasks = 0
        self.current_task = []


        thread_tcp_server = threading.Thread(target = self.manager_tcp_server)
        thread_tcp_server.name = "manager_tcp_server"
        thread_udp_server = threading.Thread(target = self.manager_udp_server)
        thread_udp_server.name = "manager_udp_server"
        
        thread_fault_tolerance = threading.Thread(target = self.fault_tolerance_thread)
        
        
        thread_tcp_server.start()
        thread_udp_server.start()

        thread_fault_tolerance.start()

        # formatter = logging.Formatter(
        #     f"Manager:{port}:%(threadName)s [%(levelname)s] %(message)s"
        # )

        

        self.run_job()
        thread_tcp_server.join()
        thread_udp_server.join()
        # thread_fault_tolerance.join()
        
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )


# phling的code：
    def manager_tcp_server (self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)

            LOGGER.info("TCP Server listening on %s:%s", self.host, self.port)

            while not self.signals["shutdown"]:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                LOGGER.info("Connection from %s", address[0])

                clientsocket.settimeout(1)

                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                print("Shutdown Message: ", message_str)
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    LOGGER.warning("Invalid JSON message received and ignored.")  # 加了为了debug
                    continue

                LOGGER.info("Received message: %s", message_dict)

                if message_dict["message_type"] == "shutdown" :  #??如果send shutdown message 遇到了connectionerror 怎么办？ mark as dead吗？
                    # send the shutdown message to all the workers
                    for worker_id in self.workers:
                        worker_host, worker_port = worker_id
                        try: 
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                # connect to the server
                                sock.connect((worker_host, worker_port))
                                # send a message
                                message = json.dumps({"message_type": "shutdown"})
                                sock.sendall(message.encode('utf-8'))
                        except ConnectionRefusedError:
                            self.workers["worker_id"]["status"] = "dead"
                            LOGGER.info("ConnectionRefusedError")

                    self.signals["shutdown"] = True
                    LOGGER.info("Manager shut down!")
                    break

                elif message_dict["message_type"] == "register":  # check the dead worker alive now
                    worker_id = (message_dict["worker_host"], message_dict["worker_port"])
                    worker_host, worker_port = worker_id
                    if worker_id in self.workers:
                        if self.workers[worker_id]["status"] == "dead" :
                            self.workers[worker_id]["status"] == "ready"
                        elif self.workers[worker_id]["status"] == "busy":
                            # reassign task 
                            self.workers[worker_id]["status"] == "ready" # !!!!!!!
                    else:
                        self.workers[worker_id] = {
                            "status": "ready", # ready, busy, dead
                            "current_task_id": None,
                            "current_task_stage": None,
                            "last_ping": time.time()
                        }
                        LOGGER.info(f"Worker registered: {worker_id}")
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                # connect to the server
                                sock.connect((worker_host, worker_port))
                                ack_message = json.dumps({"message_type": "register_ack"})
                                sock.sendall(ack_message.encode('utf-8'))
                                LOGGER.info(f"Sent registration acknowledgment to worker {worker_id}.")
                    except ConnectionRefusedError:
                        self.workers["worker_id"]["status"] = "dead"
                        LOGGER.info("ConnectionRefusedError")
                elif message_dict["message_type"] == "new_manager_job":
                    job = {
                            "job_id": self.job_count,
                            "input_directory": message_dict["input_directory"],
                            "output_directory": message_dict["output_directory"],
                            "mapper_executable": message_dict["mapper_executable"],
                            "reducer_executable": message_dict["reducer_executable"],
                            "num_mappers" : message_dict["num_mappers"],
                            "num_reducers" : message_dict["num_reducers"]
                            }
                    self.job_count += 1
                    self.job_queue.put(job)
                    LOGGER.info(f"Added Job with Job id: {job['job_id']}")
                elif message_dict["message_type"] == "finished":
                    worker_id = (message_dict["worker_host"], message_dict["worker_port"])
                    self.finished_job_tasks += 1
                    self.workers[worker_id]['status'] = "ready"
                    #"task_id": int,
    
    def manager_udp_server (self):
            # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)
            # Receive incoming UDP messages
            while not self.signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                print(message_dict)
                if message_dict["message_type"] == "heartbeat":
                    worker_host = message_dict["worker_host"]
                    worker_port = message_dict["worker_port"]
                    worker_id = (worker_host, worker_port)
                    # update last ping
                    # ??? potential use of lock
                    self.workers[worker_id]["last_ping"] = time.time()
                    # update the worker status if it was dead
                    if self.workers[worker_id]["status"] == "dead": 
                        self.workers[worker_id]["status"] = "ready"
                        LOGGER.info(f"Worker {worker_id} is alive again!")
                

    def fault_tolerance_thread (self):
        while not self.signals["shutdown"]:
            for worker in self.workers:
                if time.time() - worker["last_ping"] > 10 or worker["status"] == "dead": # the worker is dead
                    if worker["status"] == "busy":
                        worker["status"] = "dead"
                        task_id = worker["current_task_id"]
                        if worker["current_task_stage"] == "mapping":
                            # send task in tcp
                            self.send_mapping_tasks(self, job, partitions_files, tmpdir, task_map_id)  #?? 怎么pass in 这个parameter？
                        elif:
                            self.send_reducing_tasks(self, job, reduce_tasks)
                    worker["current_task_id"] = None
                    worker["current_task_stage"] = None
        time.sleep(0.1)

        
                        
    def run_job(self):
        while not self.signals["shutdown"]:
            try:
                # Wait for a job to be available in the queue or check periodically
                job = self.job_queue.get(timeout=0.1)  # Adjust timeout as necessary
                files = []
                for filename in os.listdir(job['input_directory']):
                    file_path = os.path.join(job['input_directory'], filename)
                    if os.path.isfile(file_path):
                        # Add the file to the list only if it is a regular file
                        files.append(filename)
                # Sort the list of files by name
                sorted_files = sorted(files)
                self.current_task = [[] for _ in range(job['num_mappers'])]
                for i, file_name in enumerate(sorted_files):
                    mapper_index = i % job['num_mappers']
                    self.current_task[mapper_index].append(file_name)

                    
                LOGGER.info(f"Starting job {job['job_id']}")
                # delete output directory
                output_directory = job["output_directory"]
                if os.path.exists(output_directory):
                    shutil.rmtree(output_directory)
                    LOGGER.info(f"Deleted existing output directory: {output_directory}")

                # Create the output directory
                os.makedirs(output_directory)
                LOGGER.info(f"Created output directory: {output_directory}")

                # Create a shared directory for temporary intermediate files
                prefix = f"mapreduce-shared-job{job['job_id']:05d}-" 
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    task_map_id = 0
                    # run mapping job
                    while (not self.signals["shutdown"]) and (self.finished_job_tasks != job['num_mappers']):
                        if self.current_task:
                            length = len(self.current_task)
                            self.send_mapping_tasks(job, tmpdir, task_map_id)
                            if len(self.current_task) == length - 1:
                                task_map_id += 1
                        time.sleep(0.1)

                    # create reduce tasks, this is overwritten by a new empty list
                    self.current_task = [[] for _ in range(job['num_reducers'])]
                    sorted_dir = sorted(os.listdir(tmpdir))
                    for partition_file in sorted_dir:  # partition file is "123.txt"
                        task_reduce_id = int(partition_file[-5:])
                        file_path = os.path.join(tmpdir, partition_file)
                        self.current_task[task_reduce_id].append(file_path)

                    # run reducing job
                    while (not self.signals["shutdown"]) and (self.finished_job_tasks != job['num_mappers'] + job['num_reducers']):
                        length = len(self.current_task)
                        self.send_reducing_tasks(job)
                        if len(self.current_task) == length - 1:
                            task_reduce_id += 1

                        time.sleep(0.1)
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)

            except queue.Empty:
                # No job was available in the queue
                pass
            except ConnectionRefusedError:
                self.workers["worker_id"]["status"] = "dead"
                LOGGER.info("ConnectionRefusedError")


    def send_mapping_tasks(self, job, tmpdir, task_map_id):
        for worker_id in self.workers:
            if self.workers[worker_id]['status'] == "ready":
                self.workers[worker_id]['current_task_id'] = task_map_id
                self.workers[worker_id]['current_task_stage'] = "mapping"
                self.workers[worker_id]['status'] = "busy"
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    worker_host, worker_port = worker_id
                    sock.connect((worker_host, worker_port))
                    input_paths = []
                    for file in self.current_task[0]:
                        input_paths.append(str(job['input_directory']) + '/' + str(file))
                    context = {
                                "message_type": "new_map_task",
                                "task_id": task_map_id,
                                "input_paths": input_paths,
                                "executable": job['mapper_executable'],
                                "output_directory": tmpdir,
                                "num_partitions": job['num_reducers']
                            }
                    message = json.dumps(context)
                    sock.sendall(message.encode('utf-8'))
                self.current_task.pop(0)
                break


    def send_reducing_tasks(self, job):
        for worker_id in self.workers:
            if self.workers[worker_id]['status'] == "ready":
                extract_id = self.current_task[0][0]
                task_reduce_id = int(extract_id[-5:])
                self.workers[worker_id]['current_task_id'] = task_reduce_id
                self.workers[worker_id]['current_task_stage'] = "reducing"
                self.workers[worker_id]['status'] = "busy"
                LOGGER.info("HEY there")
                LOGGER.info(self.current_task[0])
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    worker_host, worker_port = worker_id
                    sock.connect((worker_host, worker_port))
                    context = {
                                "message_type": "new_reduce_task",
                                "task_id": task_reduce_id,
                                "executable": job['reducer_executable'],
                                "input_paths": self.current_task[0],
                                "output_directory": job['output_directory'],
                            }
                    message = json.dumps(context)
                    sock.sendall(message.encode('utf-8'))
                self.current_task.pop(0)
                break



@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
