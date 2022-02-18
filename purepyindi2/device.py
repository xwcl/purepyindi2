import time
import os
import fcntl
import logging
import select
import sys
import threading
from collections import defaultdict
from queue import Queue, Empty
import typing
from .properties import IndiProperty, Role
from . import messages, constants, transports, client, properties

log = logging.getLogger(__name__)

PROPERTY_CALLBACK = typing.Callable[[properties.IndiProperty, messages.IndiDefSetDelMessage], None]

class Device:
    status : constants.ConnectionStatus = constants.ConnectionStatus.STOPPED
    sleep_interval_sec : float = 1
    _setup_complete : bool = False  # set True when setup() has run

    def __init__(self, name):
        self.name = name
        self.callbacks = defaultdict(list)
        self.connection = transports.IndiPipeConnection()
        self.properties : dict[str,IndiProperty] = {}
        self.connection.register_message_handler(self.handle_message)
        self.client = client.IndiClient(self.connection)

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

    def update_property(self, prop : IndiProperty):
        self.connection.send(prop.make_update())

    def send_all_properties(self):
        for prop_name in self.properties:
            prop = self.properties[prop_name]
            self.define_property(prop)

    def handle_message(self, message : messages.IndiMessage):
        log.debug(f"Device got {message=}")
        while not self._setup_complete:
            log.debug(f"Delaying processing of message {message} until setup completes")
            time.sleep(0.1)
        if isinstance(message, messages.GetProperties):
            if message.device == self.name:
                if message.name is not None:
                    if message.name in self.properties:
                        self.connection.send(self.properties[message.name])
                else:
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

    def main(self):
        self.connection.start()
        try:
            self.run()
        except Exception:
            self.connection.stop()
            raise
  
    def run(self):
        self.setup()
        self._setup_complete = True
        while self.connection.status is constants.ConnectionStatus.CONNECTED:
            self.loop()
            time.sleep(self.sleep_interval_sec)
    
    def loop(self):
        log.debug("device %s: running loop logic", self.name)