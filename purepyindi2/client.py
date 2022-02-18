from multiprocessing.sharedctypes import Value
import typing
import logging
log = logging.getLogger(__name__)
from collections import defaultdict
from . import constants, transports, properties, messages


class IndiClient:
    def __init__(self, connection=None):
        self._devices = defaultdict(dict)
        # funky nested defaultdict for callbacks supports 3-level lookups like
        # callbacks['device']['name'].append(callback_fn)  # property level
        # callbacks['device'][constants.ALL].append(callback_fn)  # device level
        # callbacks[constants.ALL][constants.ALL].append(callback_fn)  # world level
        self.callbacks = defaultdict(lambda: defaultdict(list))
        self._interested_properties = set()
        self.connection = connection
        if self.connection is not None:
            self.connect()

    def get_properties(self, *args):
        if len(args) == 0:
            msg = messages.GetProperties()
            self._register_interest(constants.ALL, constants.ALL)
            self.connection.send(msg)
        elif isinstance(args[0], str):
            device_name = args[0]
            if len(args) == 2:
                property_name = args[1]
            else:
                property_name = constants.ALL
            self._register_interest(device_name, property_name)
            msg = messages.GetProperties(device=device_name, name=property_name)
            self.connection.send(msg)
        elif len(args) == 1:
            for spec in args[0]:
                parts = spec.split('.')
                parts = parts[:2]
                self.get_properties(*parts)
        else:
            raise ValueError("Supply arguments as list of dotted property specs or as (device, property)")


    def connect(self, host: str=constants.DEFAULT_HOST, port: int=constants.DEFAULT_PORT):
        if self.connection is not None:
            self.connection = transports.IndiTcpConnection(host=host, port=port)
        self.connection.register_message_handler(self.handle_message)

    def _register_interest(self, device_name, property_name):
        self._interested_properties.add((device_name, property_name))

    def _add_property(self, prop):
        self._devices[prop.device][prop.name] = prop
        log.debug(f"Added {prop} to properties proxy")

    def dispatch_callbacks(self, message):
        for device_name in self.callbacks:
            for property_name in self.callbacks[device_name]:
                for cb in self.callbacks[device_name][property_name]:
                    should_fire = (
                        device_name is constants.ALL or
                        (device_name == message.device and property_name is constants.ALL) or
                        (device_name == message.device and property_name == message.name)
                    )
                    if should_fire:
                        cb(message)
                        log.debug(f"Fired {cb=}")

    def register_callback(self, cb, device_name=constants.ALL, property_name=constants.ALL):
        self.callbacks[device_name][property_name].append(cb)
        log.debug(f"Registered callback {cb=} for {device_name=} {property_name=}")

    def unregister_callback(self, cb, device_name=constants.ALL, property_name=constants.ALL):
        self.callbacks[device_name][property_name].remove(cb)
        log.debug(f"Unregistered callback {cb=} for {device_name=} {property_name=}")

    def handle_message(self, message):
        if not isinstance(message, messages.IndiDefSetDelMessage):
            return
        self.dispatch_callbacks(message)
        if isinstance(message, messages.DelProperty):
            if message.device is None:
                devices = self._devices.keys()
                log.debug(f"Deleting all properties for {list(devices)}")
                for k in devices:
                    del self._devices[k]
            else:
                device = self._devices[message.device]
                for prop in device:
                    if message.name is None or prop.name == message.name:
                        del device[message.name]
                        log.debug(f"Deleted matching {message.name} property on device {message.device}")
        elif isinstance(message, (messages.IndiDefMessage, messages.IndiSetMessage)):
            device_name = message.device
            property_name = message.name
            interested = (
                (constants.ALL, constants.ALL) in self._interested_properties or 
                (device_name, constants.ALL) in self._interested_properties or
                (device_name, property_name) in self._interested_properties
            )
            if not interested:
                return
            if device_name not in self._devices or property_name not in self._devices[device_name]:
                if isinstance(message, messages.IndiDefMessage):
                    self._devices[device_name][property_name] = properties.IndiProperty.from_definition(message)
                    log.debug(f"Constructed new property {self._devices[device_name][property_name]} from definition")
            else:
                self._devices[message.device][message.name].apply_update(message)

    def __getitem__(self, key):
        parts = key.split('.', 2)
        device_name = parts[0]
        if device_name not in self._devices:
            raise KeyError(f"No device {device_name} represented within these properties")
        device_props = self._devices[device_name]
        if len(parts) > 1:
            property_name = parts[1]
            if property_name not in device_props:
                raise KeyError(f"No property {device_name}.{property_name} represented within these properties")
            prop = device_props[property_name]
            if len(parts) > 2:
                element_name = parts[2]
                if element_name not in prop:
                    raise KeyError(f"No element {device_name}.{property_name}.{element_name} represented within these properties")
                return prop[element_name]
            else:
                return prop
        else:
            raise ValueError(f"Must supply a device.property or device.property.element string, got {key=}")

    def __setitem__(self, key, value):
        parts = key.split('.', 2)
        device_name = parts[0]
        if device_name not in self._devices:
            raise KeyError(f"No device {device_name} represented within these properties")
        device_props = self._devices[device_name]
        if len(parts) > 1:
            property_name = parts[1]
            if property_name not in device_props:
                raise KeyError(f"No property {device_name}.{property_name} represented within these properties")
            prop = device_props[property_name]
            if len(parts) > 2:
                element_name = parts[2]
                if element_name not in prop:
                    raise KeyError(f"No element {device_name}.{property_name}.{element_name} represented within these properties")
                msg = prop.make_update(**{element_name: value})
                self.connection.send(msg)
            else:
                return prop
        else:
            raise ValueError(f"Must supply a device.property or device.property.element string, got {key=}")
