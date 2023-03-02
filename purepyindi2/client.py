import logging
import dataclasses
import typing
import warnings
from collections import defaultdict
from . import constants, transports, properties, messages, utils

log = logging.getLogger(__name__)

__all__ = ['IndiClient']

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
        self.last_get_properties_scope = None
        if self.connection is not None:
            self.connect()

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.status.value}, {len(self._devices)} devices found>"

    @property
    def status(self):
        return self.connection.status if self.connection is not None else constants.ConnectionStatus.NOT_CONFIGURED

    @property
    def devices(self):
        return set(self._devices.keys())

    def to_serializable(self) -> dict[str, dict[str, properties.IndiProperty]]:
        '''Return a dict mapping device names to dicts of
        properties (dataclasses, keyed by name). Can be serialized by
        a dataclass- and enum-aware serializer.
        '''
        devices = {}
        for devname in self._devices:
            devices[devname] = {}
            thisdev = devices[devname]
            for propname in self._devices[devname]:
                thisdev[propname] = self._devices[devname][propname].to_serializable()
                # _role is always client in this context, save some bytes
                del thisdev[propname]['_role']
        return {'devices': devices}

    def to_json(self, **kwargs):
        '''Serialize devices and properties with orjson, passing
        through arguments to `orjson.dumps`. Use `to_serializable()` to
        get the dict of Python types.'''
        import orjson
        return orjson.dumps(self.to_serializable(), **kwargs)

    def get_properties(self, *args):
        '''Subscribe to some or all properties available through the
        INDI server

        Parameters
        ----------
        key_or_iterable : optional
            Default is all devices. If passed a string, it will be used
            as the device name to request properties from. If passed a
            ``device.property`` pair, then only that property will be
            requested. If passed an iterable, a ``<getProperties>``
            message will be emitted for each.
        '''
        if len(args) == 0:
            msg = messages.GetProperties()
            self._register_interest(constants.ALL, constants.ALL)
            self.connection.send(msg)
            if self.last_get_properties_scope is not None:
                warnings.warn(utils.unwrap(f"""
                    Since get_properties() was first called with
                    {self.last_get_properties_scope} (device,
                    property) scope, enumerating all devices and
                    properties is not possible without disconnecting
                    and reconnecting."""))
        elif len(args) == 1 and utils.is_iterable(args[0]) and not isinstance(args[0], str):
            for spec in args[0]:
                self.get_properties(spec)
        elif isinstance(args[0], str):
            if '.' in args[0]:
                parts = args[0].split('.')
            else:
                parts = [args[0]]
            device_name = parts[0]
            if len(parts) == 2:
                property_name = parts[1]
                msg = messages.GetProperties(device=device_name, name=property_name)
            else:
                property_name = constants.ALL
                if self.last_get_properties_scope is not None and self.last_get_properties_scope[1] is not constants.ALL:
                    warnings.warn(utils.unwrap(f"""
                        Since get_properties() was first called with
                        {self.last_get_properties_scope} (device,
                        property) scope, enumerating all properties is
                        not possible without disconnecting and
                        reconnecting."""))
                msg = messages.GetProperties(device=device_name)
            self._register_interest(device_name, property_name)
            if self.last_get_properties_scope is None:
                self.last_get_properties_scope = (device_name, property_name)
            self.connection.send(msg)
        else:
            raise ValueError("Supply arguments as list of dotted property specs or as (device, property)")


    def connect(self, host: str=constants.DEFAULT_HOST, port: int=constants.DEFAULT_PORT):
        # reset warning in case this is not the first time we're connecting
        self.last_get_properties_scope = None
        if self.connection is None:
            self.connection = transports.IndiTcpClientConnection(host=host, port=port)
        self.connection.add_callback(constants.TransportEvent.inbound, self.handle_message)
        self.connection.start()

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
                        (device_name is constants.ALL) or
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
        if not isinstance(message, typing.get_args(messages.IndiDefSetDelMessageTypes)):
            return
        self.dispatch_callbacks(message)
        if isinstance(message, messages.DelProperty):
            if message.device is None:
                devices = self._devices.keys()
                log.debug(f"Deleting all properties for {list(devices)}")
                for k in devices:
                    del self._devices[k]
            else:
                propnames = tuple(self._devices[message.device].keys())
                for propname in propnames:
                    if message.name is None or propname == message.name:
                        try:
                            del self._devices[message.device][propname]
                        except KeyError:
                            # if it was somehow deleted while we were iterating we could get a KeyError, but we were trying to delete anyway.
                            pass
                        log.debug(f"Deleted matching {propname} property on device {message.device}")
        elif isinstance(message, typing.get_args(messages.IndiDefMessage) + typing.get_args(messages.IndiSetMessage)):
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
                if isinstance(message, typing.get_args(messages.IndiDefMessage)):
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
            return {name: prop for name, prop in device_props.items()}

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
                try:
                    value = prop[element_name].value_from_text(value)
                except Exception:
                    pass  # if a proper enum value is passed in, it's not an error, but that won't go through value_from_text.
                msg = prop.make_new_property(**{element_name: value})
                self.connection.send(msg)
            else:
                return prop
        else:
            raise ValueError(f"Must supply a device.property or device.property.element string, got {key=}")
