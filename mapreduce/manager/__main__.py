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


# Configure logging
LOGGER = logging.getLogger(__name__)


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

        self.manager_tcp_server()
        self.manager_tcp_client()
        
        #thread = threading.Thread(target = self.manager_tcp_server)
        #thread.start()
        #thread.join()
        
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
                    while True:    # ????????
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

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    LOGGER.warning("Invalid JSON message received and ignored.")  # 加了为了debug
                    continue

                LOGGER.info("Received message: %s", message_dict)

                if message_dict["message_type"] == "shutdown" :
                    # send the shutdown message to all the workers
                    for worker_id in self.workers:
                        worker_host, worker_port = worker_id
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                            # connect to the server
                            sock.connect((worker_host, worker_port))
                            # send a message
                            message = json.dumps({"message_type": "shutdown"})
                            sock.sendall(message.encode('utf-8'))

                    self.signals["shutdown"] = True
                    LOGGER.info("Manager shut down!")

                elif message_dict["message_type"] == "register":
                    worker_id = (message_dict["worker_host"], message_dict["worker_port"])
                    worker_host, worker_port = worker_id
                    self.workers[worker_id] = {
                        "status": "ready",
                        # "last_ping": time.time(),
                        "current_task": None
                    }
                    LOGGER.info(f"Worker registered: {worker_id}")
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                            # connect to the server
                            sock.connect((worker_host, worker_port))
                            ack_message = json.dumps({"message_type": "register_ack"})
                            sock.sendall(ack_message.encode('utf-8'))
                            LOGGER.info(f"Sent registration acknowledgment to worker {worker_id}.")
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
                    LOGGER.info(f"Added Job: {job["job_id"]}")


    def manager_tcp_client(self):
        """Test TCP Socket Client."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # connect to the server
            sock.connect(("localhost", 8000))

            # send a message
            message = json.dumps({"hello": "world"})
            sock.sendall(message.encode('utf-8'))
    
    def run_job(self):
        while not self.signals["shutdown"]:
            try:
                # Wait for a job to be available in the queue or check periodically
                job = self.job_queue.get(timeout=1)  # Adjust timeout as necessary
                LOGGER.info(f"Starting job {job.job_id}")
                # delete output directory
                if os.path.exists(job.output_directory):
                    shutil.rmtree(job.output_directory)
                    LOGGER.info(f"Deleted existing output directory: {job.output_directory}")

                # Create the output directory
                os.makedirs(job.output_directory)
                LOGGER.info(f"Created output directory: {job.output_directory}")

                # Create a shared directory for temporary intermediate files
                prefix = f"mapreduce-shared-job{job.job_id:05d}-" 
                # prefix = f"mapreduce-shared-job{self.job_id:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    # FIXME: Change this loop so that it runs either until shutdown 
                    # or when the job is completed.
                    while (not self.signals["shutdown"]) or 
                        time.sleep(0.1)
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)

            except queue.Empty:
                # No job was available in the queue
                pass
            




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
