import socket
from functools import partial
import typing
from .constants import ALL, TransportEvent, ConnectionStatus
from .transports import IndiTcpServerListener, IndiTcpClientConnection, IndiTcpServerConnection
from .client import IndiClient
from . import messages

class IndiServerClient:
    connection : IndiTcpServerConnection
    interested_properties : set

    def __init__(self, connection, handle_inbound):
        self.connection = connection
        self.interested_properties = set()
        self.handle_inbound = handle_inbound

    def handle_client_to_server_message(self, message):
        if not isinstance(message, messages.GetProperties):
            return
        device_name = message.device if message.device is not None else ALL
        property_name = message.name if message.name is not None else ALL
        self.interested_properties.add((device_name, property_name))
        self.handle_inbound(message)

    def handle_server_to_client_message(self, message):
        if not isinstance(message, typing.get_args(messages.IndiDefSetDelMessageTypes)):
            return
        device_name = message.device
        property_name = message.name
        interested = (
            (ALL, ALL) in self.interested_properties or
            (device_name, ALL) in self.interested_properties or
            (device_name, property_name) in self.interested_properties
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
        self.clients = {}
        self.remote_server_clients = {}
        for remote_host, remote_port in remote_servers:
            c = IndiTcpClientConnection(host=remote_host, port=remote_port)
            c.start()
            self.remote_server_clients[(remote_host, remote_port)] = c
            c.add_callback(TransportEvent.inbound, self.broadcast)

    def run(self):
        self.listener.run()

    def accept_connection(self, client_socket, client_host, client_port):
        conn = IndiTcpServerConnection(host=client_host, port=client_port)
        conn.start(client_socket)
        c = IndiServerClient(conn, self.forward)
        self.clients[(client_host, client_port)] = c
        conn.add_callback(TransportEvent.connection, partial(self.client_status, client_key=(client_host, client_port)))
        conn.add_callback(TransportEvent.inbound, c.handle_client_to_server_message)

    def client_status(self, status : ConnectionStatus, client_key):
        if status is not ConnectionStatus.CONNECTED:
            del self.clients[client_key]

    def broadcast(self, indi_action):
        # TODO rewrite to r/o, filter non-visible
        for client_key in self.clients:
            self.clients[client_key].handle_server_to_client_message(indi_action)

    def forward(self, indi_action):
        for remote in self.remote_server_clients.values():
            remote.send(indi_action)