import logging
import time
import asyncio
from pprint import pformat
from collections import defaultdict
import queue
import socket
import sys
import os
import pwd
from os.path import exists
import stat
import threading
from .parser import IndiStreamParser
from .constants import (
    CHUNK_MAX_READ_SIZE,
    BLOCK_TIMEOUT_SEC,
    RECONNECTION_DELAY_SEC,
    ConnectionStatus,
    DEFAULT_HOST,
    DEFAULT_PORT,
    TransportEvent,
)

__all__ = (
    'IndiConnection',
    'IndiTcpConnection',
    'IndiPipeConnection',
    'AsyncIndiTcpConnection',
)

log = logging.getLogger(__name__)

class IndiConnection:
    QUEUE_CLASS = queue.Queue
    status : ConnectionStatus = ConnectionStatus.NOT_CONFIGURED

    def __init__(self):
        self._outbound_queue = self.QUEUE_CLASS()
        self._inbound_queue = self.QUEUE_CLASS()
        self._parser = IndiStreamParser(self._inbound_queue)
        self._writer = self._reader = None
        self.event_callbacks = defaultdict(set)
        self.callbacks_set_lock = threading.Lock()

    def add_callback(self, event: TransportEvent, callback):
        with self.callbacks_set_lock:
            self.event_callbacks[event].add(callback)

    def remove_callback(self, event: TransportEvent, callback):
        with self.callbacks_set_lock:
            self.event_callbacks[event].remove(callback)

    def dispatch_callbacks(self, event: TransportEvent, payload):
        with self.callbacks_set_lock:
            for callback in self.event_callbacks[event]:
                try:
                    callback(payload)
                except Exception as e:
                    log.exception(f"Caught exception in {event.name} callback {callback}")

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
                while True:
                    msg = self._outbound_queue.get(True, BLOCK_TIMEOUT_SEC)
                    data = msg.to_xml_bytes()
                    transport.sendall(data + b'\n')
                    log.debug(f"out: {data}")
                    self.dispatch_callbacks(TransportEvent.outbound, msg)
            except queue.Empty:
                pass
            except socket.error:
                self.status = ConnectionStatus.ERROR
                self.dispatch_callbacks(TransportEvent.disconnection, self.status)
                break
        transport.shutdown(socket.SHUT_WR)

    def _handle_inbound(self, transport):
        log.debug("Inbound handler started")
        while self.status is ConnectionStatus.CONNECTED:
            try:
                data = transport.recv(CHUNK_MAX_READ_SIZE)
                log.debug(f"in: {data}")
            except socket.timeout:
                continue
            except socket.error as e:
                log.exception("Socket error caught, disconnected")
                self.status = ConnectionStatus.ERROR
                self.dispatch_callbacks(TransportEvent.disconnection, self.status)
                break
            if data == b'':
                self.status = ConnectionStatus.STOPPED
                self.dispatch_callbacks(TransportEvent.disconnection, self.status)
                log.debug("Got EOF from server")
                break
            self._parser.parse(data)
            try:
                while True:
                    update = self._inbound_queue.get_nowait()
                    self.dispatch_callbacks(TransportEvent.inbound, update)
            except queue.Empty:
                pass
        transport.shutdown(socket.SHUT_RD)

    def _start_reader_writer_threads(self):
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

class IndiTcpClientConnection(IndiTcpConnection):
    """Connection from client to server with optional reconnection logic"""
    reconnect_automatically : bool = True
    def __init__(self, *args, reconnect_automatically=None, **kwargs):
        self._monitor = None
        if reconnect_automatically is not None:
            self.reconnect_automatically = reconnect_automatically
        super().__init__(*args, **kwargs)
    def _reconnection_monitor(self):
        while self.status is not ConnectionStatus.STOPPED:
            log.debug("Creating socket...")
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            log.debug(f"Created {self._socket}")
            try:
                log.debug(f"Opening {self.host}:{self.port}...")
                self._socket.connect((self.host, self.port))
                log.debug(f"Opened {self.host}:{self.port}")
                self._socket.settimeout(BLOCK_TIMEOUT_SEC)
                log.debug(f"Set timeout to {BLOCK_TIMEOUT_SEC}")
                self.status = ConnectionStatus.CONNECTED
                log.info(f"Connected to {self.host}:{self.port}")
                self.dispatch_callbacks(TransportEvent.connection, self.status)
                log.debug("Connection state change callbacks dispatched.")
            except ConnectionError as e:
                log.debug("Closing socket...")
                try:
                    self._socket.close()
                    log.debug("Socket closed")
                except Exception as e:
                    log.exception("Unable to close socket")
                log.error(f"Failed to connect to {self.host}:{self.port} [{e}]")
                if self.reconnect_automatically:
                    log.info(f"Retrying in {RECONNECTION_DELAY_SEC} sec...")
                    time.sleep(RECONNECTION_DELAY_SEC)
                    continue
                else:
                    self.status = ConnectionStatus.ERROR
                    self.dispatch_callbacks(TransportEvent.disconnection, self.status)
                    raise

            self._start_reader_writer_threads()
            self._writer.join()
            self._reader.join()

            if not self.reconnect_automatically:
                self.status = ConnectionStatus.ERROR
                self.dispatch_callbacks(TransportEvent.disconnection, self.status)
                raise ConnectionError("Connection failed and reconnect_automatically is False")
            else:
                self.status = ConnectionStatus.CONNECTING
                self.dispatch_callbacks(TransportEvent.disconnection, self.status)
                log.error(f"Connection failed. Reconnecting to {self.host}:{self.port} in {RECONNECTION_DELAY_SEC} sec...")
                time.sleep(RECONNECTION_DELAY_SEC)

    def start(self):
        if self.status is not ConnectionStatus.CONNECTED:
            self._monitor = threading.Thread(
                target=self._reconnection_monitor,
                name=f'{self.__class__.__name__}-monitor',
                daemon=True,
            )
            self._monitor.start()

    def stop(self):
        if self.status is ConnectionStatus.CONNECTED:
            self.status = ConnectionStatus.STOPPED
            self.dispatch_callbacks(TransportEvent.disconnection, self.status)
            self._reconnection_monitor.join(BLOCK_TIMEOUT_SEC)
            self._writer = None
            self._reader = None

class IndiTcpServerConnection(IndiTcpConnection):
    '''Connection used by a server to communicate with a single client'''
    def start(self, client_socket):
        if self.status is not ConnectionStatus.CONNECTED:
            self._socket = client_socket
            self.status = ConnectionStatus.CONNECTED
            self._start_reader_writer_threads()
        else:
            raise RuntimeError("start() called twice without stop()")
    def stop(self):
        if self.status is ConnectionStatus.CONNECTED:
            self.status = ConnectionStatus.STOPPED
            self.dispatch_callbacks(TransportEvent.disconnection, self.status)
            self._writer.join(BLOCK_TIMEOUT_SEC)
            self._writer = None
            self._reader.join(BLOCK_TIMEOUT_SEC)
            self._reader = None

class IndiTcpServerListener:
    '''Listener that binds a socket to accept incoming connections'''
    status : ConnectionStatus = ConnectionStatus.NOT_CONFIGURED
    listening_socket : socket.socket
    clients : dict[tuple[str, int], IndiTcpServerConnection]
    bind_to : tuple[str, int]
    def __init__(self, bind_to, accept_socket_callback):
        self.accept_socket_callback = accept_socket_callback
        self.bind_to = bind_to
        self.listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients = {}

    def run(self):
        self.status = ConnectionStatus.CONNECTED
        self.listening_socket.bind(self.bind_to)
        while self.status is not ConnectionStatus.STOPPED:
            self.listening_socket.listen()
            client_socket, (client_host, client_port) = self.listening_socket.accept()
            self.accept_socket_callback(client_socket, client_host, client_port)

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
                message_str = res.to_xml_str()
                transport.write(message_str + '\n')
                log.debug(f"out: {repr(message_str)}")
                transport.flush()
                self.dispatch_callbacks(TransportEvent.outbound, res)
            except queue.Empty:
                pass

    def _handle_inbound(self, transport):
        log.debug("Inbound handler started")
        while self.status is ConnectionStatus.CONNECTED:
            from_server = transport.readline(CHUNK_MAX_READ_SIZE)
            log.debug(f"in: {repr(from_server)}")
            self._parser.parse(from_server)
            try:
                while True:
                    update = self._inbound_queue.get_nowait()
                    self.dispatch_callbacks(TransportEvent.inbound, update)
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
            self._writer.join(BLOCK_TIMEOUT_SEC)
            self._reader.join(BLOCK_TIMEOUT_SEC)

def is_fifo(path):
    return stat.S_ISFIFO(os.stat(path).st_mode)

def make_fifo_and_open(path, mode):
    if exists(path):
        if not is_fifo(path):
            raise RuntimeError(f"{path} exists and is not a FIFO")
    else:
        os.mkfifo(path, mode=(
            stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP
        ))
    fd = os.open(path, os.O_RDWR)
    return os.fdopen(fd, mode)

class IndiFifoConnection(IndiPipeConnection):
    MAGAOX_USER = 'xsup'

    def _make_and_open_fifos(self):
        current_user = pwd.getpwuid(os.getuid()).pw_name
        if current_user != self.MAGAOX_USER:
            raise RuntimeError(f"Magical MagAO-X FIFOs must only be made by xsup, but you appear to be running as {current_user}")
        return (
            make_fifo_and_open(self.input_fifo_path, 'r'),
            make_fifo_and_open(self.output_fifo_path, 'w'),
            make_fifo_and_open(self.control_fifo_path, 'w'),
        )
    def __init__(self, *args, name=None, fifos_root="/tmp", **kwargs):
        if name is None:
            raise RuntimeError("Name must be supplied for FIFO transport")
        self.input_fifo_path = os.path.join(fifos_root, f"{name}.in")
        self.output_fifo_path = os.path.join(fifos_root, f"{name}.out")
        self.control_fifo_path = os.path.join(fifos_root, f"{name}.ctrl")
        input_pipe, output_pipe, self.control_pipe = self._make_and_open_fifos()
        super().__init__(*args, input_pipe=input_pipe, output_pipe=output_pipe, **kwargs)

    def start(self):
        if not self.status is ConnectionStatus.CONNECTED:
            self.control_pipe.write('1')
            self.control_pipe.flush()
            super().start()

class AsyncIndiTcpConnection(IndiTcpConnection):
    QUEUE_CLASS = asyncio.Queue
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.async_event_callbacks = defaultdict(set)
        self.async_callbacks_set_lock = asyncio.Lock()

    async def add_async_callback(self, event: TransportEvent, callback):
        async with self.async_callbacks_set_lock:
            self.async_event_callbacks[event].add(callback)

    async def remove_async_callback(self, event: TransportEvent, callback):
        async with self.async_callbacks_set_lock:
            self.async_event_callbacks[event].remove(callback)

    async def dispatch_async_callbacks(self, event: TransportEvent, payload):
        async with self.async_callbacks_set_lock:
            for callback in self.async_event_callbacks[event]:
                try:
                    await callback(payload)
                except Exception as e:
                    log.exception(f"Caught exception in {event.name} callback {callback}")

    def start(self):
        log.debug("To start, schedule an async task for AsyncINDIClient.run")

    async def run(self, reconnect_automatically=False):
        while self.status is not ConnectionStatus.STOPPED:
            log.info(f"Starting connection to {self.host}:{self.port}")
            try:
                reader_handle, writer_handle = await asyncio.open_connection(
                    self.host,
                    self.port
                )
                addr = writer_handle.get_extra_info("peername")
                log.info(f"Connected to {addr!r}")
                self.status = ConnectionStatus.CONNECTED
                self.dispatch_callbacks(TransportEvent.connection, self.status)
                await self.dispatch_async_callbacks(TransportEvent.connection, self.status)
                self._reader = asyncio.ensure_future(self._handle_inbound(reader_handle))
                self._writer = asyncio.ensure_future(self._handle_outbound(writer_handle))
                try:
                    await asyncio.gather(
                        self._reader, self._writer
                    )
                except asyncio.CancelledError:
                    continue
            except Exception as e:
                log.exception(f"Exception in {self.__class__.__name__}")
                if reconnect_automatically:
                    log.info(f"Retrying in {RECONNECTION_DELAY_SEC} seconds")
                    self.status = ConnectionStatus.CONNECTING
                    log.info("Connection state changed to RECONNECTING")
                else:
                    self.status = ConnectionStatus.ERROR
                    log.info("Connection state changed to ERROR")
                self.dispatch_callbacks(TransportEvent.disconnection, self.status)
                log.debug("Dispatched callbacks for connection state change")
                await self.dispatch_async_callbacks(TransportEvent.disconnection, self.status)
                log.debug("Dispatched async callbacks for connection state change")
                if reconnect_automatically:
                    log.debug(f"Going to sleep for {RECONNECTION_DELAY_SEC} sec")
                    await asyncio.sleep(RECONNECTION_DELAY_SEC)
                else:
                    raise ConnectionError(f"Got disconnected from {self.host}:{self.port}, not attempting reconnection (Original exception was: {type(e)}, {e})")
            finally:
                self._cancel_tasks()
    def _cancel_tasks(self):
        if self._reader is not None:
            self._reader.cancel()
        if self._writer is not None:
            self._writer.cancel()
    async def stop(self):
        self.status = ConnectionStatus.STOPPED
        self._cancel_tasks()
    async def _handle_inbound(self, reader_handle):
        while self.status == ConnectionStatus.CONNECTED:
            try:
                data = await asyncio.wait_for(reader_handle.read(CHUNK_MAX_READ_SIZE), BLOCK_TIMEOUT_SEC)
                log.debug(f"in: {data}")
            except asyncio.TimeoutError:
                log.debug(f"No data for {BLOCK_TIMEOUT_SEC} sec")
                continue
            if data == b'':
                log.debug("Got EOF from server")
                raise ConnectionError("Got EOF from server")
            self._parser.parse(data)
            while not self._inbound_queue.empty():
                update = await self._inbound_queue.get()
                log.debug(f"Got update:\n{pformat(update)}")
                self.dispatch_callbacks(TransportEvent.inbound, update)
                await self.dispatch_async_callbacks(TransportEvent.inbound, update)

    async def _handle_outbound(self, writer_handle):
        while self.status == ConnectionStatus.CONNECTED:
            try:
                message = await self._outbound_queue.get()
                data = message.to_xml_bytes()
                writer_handle.write(data + b'\n')
                await writer_handle.drain()
                log.debug(f"out: {data}")
                self.dispatch_callbacks(TransportEvent.outbound, message)
                await self.dispatch_async_callbacks(TransportEvent.outbound, message)
            except asyncio.CancelledError:
                writer_handle.close()
                await writer_handle.wait_closed()
                raise
