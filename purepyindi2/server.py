import socket
import typing
from .constants import ALL, TransportEvent
from .transports import IndiTcpServerListener, IndiTcpClientConnection, IndiTcpServerConnection
from .client import IndiClient
from . import messages

class IndiServerClient:
    connection : IndiTcpServerConnection
    interested_properties : set

    def __init__(self, connection, client_socket):
        self.connection = connection
        self.connection.start(client_socket)

    def handle_client_to_server_message(self, message):
        if not isinstance(message, messages.GetProperties):
            return
        device_name = message.device if message.device is not None else ALL
        property_name = message.name if message.name is not None else ALL
        self.interested_properties.add((device_name, property_name))

    def handle_server_to_client_message(self, message):
        if not isinstance(message, messages.IndiDefSetDelMessage):
            return
        device_name = message.device
        property_name = message.name
        interested = (
            (ALL, ALL) in self._interested_properties or
            (device_name, ALL) in self._interested_properties or
            (device_name, property_name) in self._interested_properties
        )
        if interested:
            self.connection.send(message)

class IndiServer:
    remote_server_conns : dict[tuple[str, int], IndiTcpClientConnection]
    listener : IndiTcpServerListener
    clients : dict[tuple[str, int], IndiTcpServerConnection]

    def __init__(
        self,
        bind_host: str,
        bind_port: int,
        remote_servers: list[tuple[str, int]],
        settable: typing.Optional[list[str]] = None,
        visible: typing.Union[list[str], ALL, None] = ALL,
    ):
        self.listener = IndiTcpServerListener((bind_host, bind_port), self.accept_connection)
        self.clients = []
        self.remote_server_clients = {}
        for remote_host, remote_port in remote_servers:
            c = IndiTcpClientConnection(host=remote_host, port=remote_port)
            c.start()
            msg = messages.GetProperties()
            c.send(msg)
            self.remote_server_clients[(remote_host, remote_port)] = c
            c.add_callback(TransportEvent.inbound, self.broadcast)

    def start(self):
        self.listener.start()

    def accept_connection(self, client_socket, client_host, client_port):
        conn = IndiTcpServerConnection(host=client_host, port=client_port)
        conn.start(client_socket)
        self.clients[(client_host, client_port)] = conn

    def client_disconnect(self, client_key):
        del self.clients[client_key]

    def broadcast(self, indi_action):
        # TODO rewrite to r/o, filter non-visible
        for client_key in self.clients:
            self.clients[client_key].send(indi_action)
