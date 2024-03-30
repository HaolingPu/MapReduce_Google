"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import socket
import threading
import tempfile
import hashlib
import subprocess
import shutil
import heapq
from contextlib import ExitStack
from functools import lru_cache
import click

# 2. self.worker  is not inserted!
# 3. I have infinite loop for the fault tolarance. WHY?
# 3. how to initailize worker's "last_ping"  ?time.time()

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
        self.send_heartbeat = False
        thread_tcp_server = threading.Thread(target=self.worker_tcp_server)
        thread_tcp_server.name = "worker_thread"
        worker_udp_client = threading.Thread(target=self.worker_udp_client)
        thread_tcp_server.start()
        # while not self.signals["shutdown"]:
        while not self.signals["shutdown"]:
            if self.send_heartbeat is True:
                worker_udp_client.start()
                break
            time.sleep(0.1)

        thread_tcp_server.join()

        if worker_udp_client.is_alive():
            worker_udp_client.join()

    @lru_cache(maxsize=1024)
    def hash_key(self, key):
        """Cache and return the hash of the key."""
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), base=16)

    def worker_tcp_server(self):
        """Send a registration message to the Manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)
            self.worker_tcp_ack()

            while not self.signals["shutdown"]:
                # Wait for a connection for 1s.
                # The socket library avoids consuming
                # CPU while waiting for a connection.
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

                LOGGER.info(message_str)
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    LOGGER.error("Failed to decode JSON message.")
                    continue
                LOGGER.info(message_dict)

                if message_dict["message_type"] == "register_ack":
                    # use a flag here
                    self.send_heartbeat = True

                elif message_dict["message_type"] == "shutdown":
                    # if worker is busy, shutdown after running job
                    print("Worker SHUTDOWN!")
                    self.signals["shutdown"] = True
                    break
                #  else do work
                elif message_dict["message_type"] == "new_map_task":
                    self.mapper_worker(message_dict)
                    self.send_finished_message(message_dict['task_id'])
                elif message_dict["message_type"] == "new_reduce_task":
                    self.reducer_worker(message_dict)
                    self.send_finished_message(message_dict['task_id'])

    def worker_udp_client(self):
        """Send heartbeat every 2 seconds."""
        while not self.signals["shutdown"]:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                # Connect to the UDP socket on server
                sock.connect((self.manager_host, self.manager_port))
                context = {
                        "message_type": "heartbeat",
                        "worker_host": self.host,
                        "worker_port": self.port
                }
                message = json.dumps(context)
                sock.sendall(message.encode('utf-8'))
                time.sleep(2)

    def mapper_worker(self, map_task):
        """Send a registration message to the Manager."""
        task_id = map_task['task_id']
        map_executable = map_task['executable']
        input_paths = map_task['input_paths']
        shared_dir = map_task['output_directory']
        num_partitions = map_task['num_partitions']
        partition_files = []
        # create directory local to worker
        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(
            prefix=prefix
        ) as tmpdir, ExitStack() as stack:
            LOGGER.info("Created local tmpdir %s", tmpdir)
            # partition_files = {}  # Track file descriptors
            # run executable on each file
            for i in range(num_partitions):
                partition_path = os.path.join(
                                tmpdir,
                                f"maptask{task_id:05d}-part{i:05d}"
                )
                partition_files.append(stack.enter_context(open(partition_path, 'w', encoding = "utf-8")))
            for input_path in input_paths:
                # infile = stack.enter_context(
                #     open(input_path, 'r', encoding='utf-8')
                # )
                with stack.enter_context(open(input_path, 'r', encoding='utf-8')) as infile:
                    with subprocess.Popen(
                        [map_executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True
                    ) as process:  # TIME ISSUE
                        for line in process.stdout:
                            # partition
                            key = line.partition("\t")[0]
                            keyhash = self.hash_key(key)
                            partition_number = keyhash % num_partitions
                            partition_files[partition_number].write(line)

            for f in partition_files:
                f.close()
                # Sort and move files to shared directory
            for partitionfiles in os.listdir(tmpdir):
                file_path = os.path.join(tmpdir, partitionfiles)
                # Sort file
                subprocess.run(
                    ['sort', '-o', file_path, file_path], check=True
                )
                # Move to shared directory
                shutil.move(file_path, shared_dir)
                # os.path.join(shared_dir, partitionfiles

        LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def send_finished_message(self, task_id):
        """Send a registration message to the Manager."""
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
        LOGGER.info("Sent 'finished' message for task %s.", task_id)

    def reducer_worker(self, reduce_task):
        """Send a registration message to the Manager."""
        task_id = reduce_task["task_id"]
        reduce_executable = reduce_task['executable']
        input_paths = reduce_task['input_paths']  # a list of paths
        output_dir = reduce_task['output_directory']

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(
            prefix=prefix
        ) as tmpdir, ExitStack() as stack:
            LOGGER.info("Created local tmpdir %s", tmpdir)
            input_files = []
            for file in input_paths:
                input_files.append(
                    stack.enter_context(
                        open(file, encoding='utf-8')
                    )
                )
                
            part_num = input_paths[0][-5:]
            output_path = os.path.join(tmpdir, f"part-{part_num}")
            with stack.enter_context(open(output_path, 'w', encoding="utf-8")) as output_file:
                with subprocess.Popen(
                        [reduce_executable],
                        text=True,
                        stdin=subprocess.PIPE,
                        stdout=output_file
                ) as reduce_process:  # TIME ISSUE
                    # Pipe input to reduce_process
                    for line in heapq.merge(*input_files):
                        reduce_process.stdin.write(line)

                #for file in input_files:
                #    file.close()
            # Move to shared directory[]
            shutil.move(output_path, output_dir)

        LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def worker_tcp_ack(self):
        """Send a registration message to the Manager."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.manager_host, self.manager_port))
                message = json.dumps({
                    "message_type": "register",
                    "worker_host": self.host,
                    "worker_port": self.port
                })
                sock.sendall(message.encode('utf-8'))
                LOGGER.info("Sent register message to Manager")
        except ConnectionRefusedError:
            LOGGER.info("ConnectionRefusedError")


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
