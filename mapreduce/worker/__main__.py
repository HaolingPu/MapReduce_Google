"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import mapreduce.utils
import socket
import threading
import tempfile
import hashlib
import subprocess
import shutil


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.signals = {"shutdown": False}


        thread_tcp_server = threading.Thread(target = self.worker_tcp_server)
        thread_tcp_server.name = "worker_thread"
        thread_tcp_server.start()

        thread_tcp_server.join()


    def worker_tcp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)
            self.worker_tcp_ack()

            while not self.signals["shutdown"] :
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                LOGGER.info(f"Connection from {address[0]}")

                
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

                LOGGER.info(message_str)
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    LOGGER.error("Failed to decode JSON message.")
                    continue
                LOGGER.info(message_dict)

                if message_dict["message_type"] == "shutdown":
                    # if worker is busy, shutdown after running job
                    self.signals["shutdown"] = True
                    break
                #  else do work
                elif message_dict["message_type"] == "new_map_task":
                    self.mapper_worker(message_dict)
                    self.send_finished_message(message_dict['task_id'])


    def mapper_worker(self, map_task):
        task_id = map_task['task_id']
        map_executable = map_task['executable']
        input_paths = map_task['input_paths']
        shared_dir = map_task['output_directory']
        num_partitions = map_task['num_partitions']
        # create directory local to worker
        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created local tmpdir %s", tmpdir)
            partition_files = {}  # Track open file descriptors
            # run executable on each file
            for input_path in input_paths:
                with open(input_path, 'r') as infile:
                    with subprocess.Popen(
                        [map_executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True
                    ) as process:
                        for line in process.stdout:
                            # partition
                            key, value = line.split('\t', 1)
                            hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            partition_number = keyhash % num_partitions
                            partition_path = os.path.join(tmpdir, f"maptask{task_id:05d}-part{partition_number:05d}")
                            #如果没有才需要加这个 direction
                            if partition_path not in partition_files:
                                partition_files[partition_path] = open(partition_path, 'a')
                            partition_files[partition_path].write(line)
                            # with open(partition_path, 'w') as file:
                            #     file.write(line.decode('utf-8') + '\n') # write key value pair into part
                            
            for f in partition_files.values():
                f.close()
                # Sort and move files to shared directory
            for partitionfiles in os.listdir(tmpdir):
                file_path = os.path.join(tmpdir, partitionfiles)
                # Sort file
                subprocess.run(['sort', '-o',file_path, file_path], check = True)
                # Move to shared directory
                shutil.move(file_path, os.path.join(shared_dir, partitionfiles))

        LOGGER.info("Cleaned up tmpdir %s", tmpdir)


    def send_finished_message(self, task_id):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Establish connection to the Manager's main socket
            sock.connect((self.manager_host, self.manager_port))
            finished_message = {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port,
            }
            message_str = json.dumps(finished_message)
            sock.sendall(message_str.encode('utf-8'))
        LOGGER.info(f"Sent 'finished' message for task {task_id}.")
        
                            
                    
    def worker_tcp_ack(self):
        """Send a registration message to the Manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps({      # 需要写 “finish”
                "message_type": "register",
                "worker_host": self.host,
                "worker_port": self.port
            })
            sock.sendall(message.encode('utf-8'))
            LOGGER.info("Sent register message to Manager")
            



@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)

if __name__ == "__main__":
    main()
