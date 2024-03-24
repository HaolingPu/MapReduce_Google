"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import socket


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        self.host = host
        self.port = port
        self.signals = {"shutdown": False}
        self.workers = []

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # TODO:
        self.manager_tcp_server()


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
            # Socket recv() will block for a maximum of 1 second.  If you omit
            # this, it blocks indefinitely, waiting for packets.
                clientsocket.settimeout(1)
                # Receive data, one chunk at a time.  If recv() times out before we
            # can read a chunk, then go back to the top of the loop and try
            # again.  When the client closes the connection, recv() returns
            # empty data, which breaks out of the loop.  We make a simplifying
            # assumption that the client will always cleanly close the
            # connection.
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
                    for worker in self.workers:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                            # connect to the server
                            sock.connect((worker.host, worker.port))
                            # send a message
                            message = json.dumps({"shutdown": True})
                            sock.sendall(message.encode('utf-8'))

                    self.signals["shutdown"] = True
                    LOGGER.info("Manager shut down!")
                                



    def manager_tcp_client(self):
        """Test TCP Socket Client."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # connect to the server
            sock.connect(("localhost", 8000))

            # send a message
            message = json.dumps({"hello": "world"})
            sock.sendall(message.encode('utf-8'))




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
