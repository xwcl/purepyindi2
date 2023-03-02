import time
import typing
from functools import partial
import logging
from collections import defaultdict
import typing
from .properties import IndiProperty
from . import messages, constants, transports, client, properties
from .client import IndiClient

log = logging.getLogger(__name__)

PROPERTY_CALLBACK = typing.Callable[[properties.IndiProperty, messages.IndiDefSetDelMessage], None]

class MockClient:
    def __getattr__(self, name):
        raise RuntimeError(f"Tried to access {name} attribute of {self} before the client connection had started")

class Device:
    name : str
    sleep_interval_sec : float = 1
    _setup_complete : bool = False  # set True when setup() has run
    client : typing.Optional[IndiClient] = MockClient()

    def __init__(self, name, connection_class=transports.IndiPipeConnection):
        self.name = name
        self.callbacks = defaultdict(list)
        self.connection = connection_class()
        self.properties : dict[str,IndiProperty] = {}
        self.connection.add_callback(constants.TransportEvent.inbound, self.handle_message)

    def add_property(
        self, 
        new_property : IndiProperty,
        *,
        callback : typing.Optional[PROPERTY_CALLBACK]=None
    ):
        if new_property.name in self.properties:
            raise ValueError(f"Name {new_property.name} conflicts with existing property")
        new_property._role = constants.Role.DEVICE
        new_property.device = self.name
        self.properties[new_property.name] = new_property
        if callback is not None:
            self.callbacks[new_property.name].append(callback)

    def define_property(self, prop : IndiProperty):
        self.connection.send(prop)

    def delete_property(self, prop : IndiProperty):
        self.connection.send(messages.DelProperty(device=self.name, name=prop.name))

    def update_property(self, prop : IndiProperty):
        self.connection.send(prop.make_set_property())

    def send_all_properties(self):
        for prop_name in self.properties:
            prop = self.properties[prop_name]
            log.debug(f"Sending {prop=}")
            self.define_property(prop)

    def handle_message(self, message : messages.IndiMessage):
        log.debug(f"Device got {message=}")
        while not self._setup_complete:
            log.debug(f"Delaying processing of message {message} until setup completes")
            time.sleep(0.1)
        if isinstance(message, messages.GetProperties):
            log.debug("Get properties got")
            if message.device is None:
                log.debug("Sending all properties (catch-all getProperties)")
                self.send_all_properties()
            elif message.device == self.name:
                if message.name is not None:
                    if message.name in self.properties:
                        self.connection.send(self.properties[message.name])
                else:
                    log.debug(f"Sending all properties (for device {message.device})")
                    self.send_all_properties()
        elif isinstance(message, typing.get_args(messages.IndiNewMessage)):
            if message.device == self.name and message.name in self.properties:
                for cb in self.callbacks[message.name]:
                    try:
                        cb(self.properties[message.name], message)
                        log.debug(f"Fired callback {cb=} with {message=}")
                    except Exception:
                        log.exception(f"Caught exception from property {message.name} callback {cb}")

    def setup(self):
        pass

    def teardown(self):
        pass

    def delete_all_properties(self):
        for prop_name in self.properties:
            self.delete_property(self.properties[prop_name])
            log.debug(f"Deleted {self.properties[prop_name]}")

    def main(self):
        self.connection.start()
        try:
            self.run()
        finally:
            self.teardown()
            log.debug("Teardown complete")
            self.delete_all_properties()
            self.connection.stop()
            log.debug("Connection stopped")
  
    def run(self):
        self.client = client.IndiClient()
        self.setup()
        self.send_all_properties()
        self._setup_complete = True
        while self.connection.status is constants.ConnectionStatus.CONNECTED:
            self.loop()
            time.sleep(self.sleep_interval_sec)
    
    def loop(self):
        log.debug("device %s: running loop logic", self.name)


class XDevice(Device):
    def __init__(self, name, *args, fifos_root="/opt/MagAOX/drivers/fifos", **kwargs):
        super().__init__(name, *args, connection_class=partial(transports.IndiFifoConnection, name=name, fifos_root=fifos_root), **kwargs)