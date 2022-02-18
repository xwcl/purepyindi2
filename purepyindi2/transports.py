import logging
from pprint import pformat
import queue
import socket
import sys
import threading
from .parser import IndiStreamParser
from .constants import (
    CHUNK_MAX_READ_SIZE,
    BLOCK_TIMEOUT_SEC,
    ConnectionStatus,
    DEFAULT_HOST,
    DEFAULT_PORT,
)

log = logging.getLogger(__name__)

class IndiConnection:
    QUEUE_CLASS = queue.Queue

    def __init__(self):
        self.status = ConnectionStatus.STARTING
        self._outbound_queue = self.QUEUE_CLASS()
        self._inbound_queue = self.QUEUE_CLASS()
        self._parser = IndiStreamParser(self._inbound_queue)
        self._writer = self._reader = None
        self.inbound_message_handlers = set()

    def register_message_handler(self, handler):
        self.inbound_message_handlers.add(handler)

    def handle_message(self, message):
        for handler in self.inbound_message_handlers:
            try:
                handler(message)
            except Exception as e:
                log.exception(f"Caught exception in an inbound message handler {handler}")

    def send(self, indi_action):
        self._outbound_queue.put_nowait(indi_action)

    def _handle_outbound(self, transport):
        raise NotImplementedError()

    def _handle_inbound(self, transport):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class IndiTcpConnection(IndiConnection):
    def __init__(self, *args, host=DEFAULT_HOST, port=DEFAULT_PORT, **kwargs):
        self.host, self.port = host, port
        super().__init__(*args, **kwargs)

    def _handle_outbound(self, transport : socket.socket):
        log.debug("Outbound handler started")
        while self.status is ConnectionStatus.CONNECTED:
            try:
                msg = self._outbound_queue.get(True, BLOCK_TIMEOUT_SEC)
                data = msg.to_xml_bytes()
                transport.sendall(data + b'\n')
            except queue.Empty:
                pass

    def _handle_inbound(self, transport):
        log.debug("Inbound handler started")
        while not self.status == ConnectionStatus.STOPPED:
            try:
                data = transport.recv(CHUNK_MAX_READ_SIZE)
            except socket.timeout:
                continue
            self._parser.parse(data)
            try:
                while True:
                    update = self._inbound_queue.get_nowait()
                    self.handle_message(update)
            except queue.Empty:
                pass

    def start(self):
        if self.status is not ConnectionStatus.CONNECTED:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self.host, self.port))
            self._socket.settimeout(BLOCK_TIMEOUT_SEC)
            self.status = ConnectionStatus.CONNECTED
            log.debug(f"Connected to {self.host}:{self.port}")
            self._writer = threading.Thread(
                target=self._handle_outbound,
                name=f'{self.__class__.__name__}-sender',
                daemon=True,
                args=(self._socket,)
            )
            self._writer.start()
            self._reader = threading.Thread(
                target=self._handle_inbound,
                name=f'{self.__class__.__name__}-receiver',
                daemon=True,
                args=(self._socket,)
            )
            self._reader.start()

    def stop(self):
        if self.status is ConnectionStatus.CONNECTED:
            self.status = ConnectionStatus.STOPPED
            self._writer.join()
            self._reader.join()
            self._writer = None
            self._reader = None


class IndiPipeConnection(IndiConnection):
    def __init__(self, *args, input_pipe=None, output_pipe=None, **kwargs):
        self.input_pipe = input_pipe if input_pipe is not None else sys.stdin
        self.output_pipe = output_pipe if output_pipe is not None else sys.stdout
        super().__init__(*args, **kwargs)

    def _handle_outbound(self, transport):
        log.debug("Outbound handler started")
        while self.status is ConnectionStatus.CONNECTED:
            try:
                res = self._outbound_queue.get(True, BLOCK_TIMEOUT_SEC)
                transport.write(res.to_xml_str() + '\n')
                transport.flush()
            except queue.Empty:
                pass

    def _handle_inbound(self, transport):
        log.debug("Inbound handler started")
        while self.status is ConnectionStatus.CONNECTED:
            from_server = transport.readline(CHUNK_MAX_READ_SIZE)
            self._parser.parse(from_server)
            try:
                while True:
                    update = self._inbound_queue.get_nowait()
                    self.handle_message(update)
            except queue.Empty:
                pass

    def start(self):
        if not self.status is ConnectionStatus.CONNECTED:
            self.status = ConnectionStatus.CONNECTED
            self._writer = threading.Thread(
                target=self._handle_outbound,
                name=f'{self.__class__.__name__}-sender',
                daemon=True,
                args=(self.output_pipe,)
            )
            self._writer.start()
            self._reader = threading.Thread(
                target=self._handle_inbound,
                name=f'{self.__class__.__name__}-receiver',
                daemon=True,
                args=(self.input_pipe,)
            )
            self._reader.start()

    def stop(self):
        if self.status is ConnectionStatus.CONNECTED:
            self.status = ConnectionStatus.STOPPED
            self._writer.join()
            self._reader.join()
