import logging
import threading
import time
import typing
import warnings
from collections import defaultdict
from . import constants, transports, properties, messages, utils

log = logging.getLogger(__name__)

__all__ = ['IndiClient']

class RemoteDevice(defaultdict):
    name : str
    def __init__(self, name):
        self.name = name

    def __repr__(self) -> str:
        out = ""
        for prop_key in sorted(self.keys()):
            for elem_key in sorted(self[prop_key]):
                out += f"{self.name}.{prop_key}.{elem_key}={self[prop_key][elem_key]}\n"
        return out

class RemoteDevices:
    _devices : defaultdict[str, RemoteDevice]
    def __init__(self):
        self._devices = defaultdict(RemoteDevice)

    def __iter__(self):
        for devname in self._devices:
            yield devname

    def __getitem__(self, key):
        if key in self._devices:
            return self._devices[key]
        else:
            raise KeyError(f"No device named {key} represented in these properties (Maybe you need to get_properties({repr(key)})?)")

    def __setitem__(self, key, value):
        self._devices[key] = value

    def __delitem__(self, key):
        del self._devices[key]

    @property
    def names(self):
        return set(self._devices.keys())

    def __repr__(self):
        out = ""
        for k in sorted(self._devices):
            out += repr(self._devices[k])
        return out

class IndiClient:
    _has_connected_once : bool = False
    def __init__(self, connection=None):
        self.devices = RemoteDevices()
        # funky nested defaultdict for callbacks supports 3-level lookups like
        # callbacks['device']['name'].append(callback_fn)  # property level
        # callbacks['device'][constants.ALL].append(callback_fn)  # device level
        # callbacks[constants.ALL][constants.ALL].append(callback_fn)  # world level
        self.callbacks = defaultdict(lambda: defaultdict(list))
        self.callbacks_lock = threading.Lock()
        self._interested_properties = set()
        self.connection = connection
        self.last_get_properties_scope = None
        if self.connection is not None:
            self.connect()

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.status.value}, {len(self.devices.names)} devices found>"

    @property
    def status(self):
        return self.connection.status if self.connection is not None else constants.ConnectionStatus.NOT_CONFIGURED

    def to_serializable(self) -> dict[str, dict[str, properties.IndiProperty]]:
        '''Return a dict mapping device names to dicts of
        properties (dataclasses, keyed by name). Can be serialized by
        a dataclass- and enum-aware serializer.
        '''
        devices = {}
        for devname in self.devices.names:
            devices[devname] = {}
            thisdev = devices[devname]
            for propname in self.devices[devname]:
                thisdev[propname] = self.devices[devname][propname].to_serializable()
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

    @property
    def interested_properties_missing(self):
        any_missing = False
        for device_name, property_name in self._interested_properties:
            log.debug(f"{device_name=} {property_name=} {len(self.devices.names)=} {len(self.devices._devices.get(device_name, []))=}")
            if device_name is constants.ALL and len(self.devices.names) == 0:
                any_missing = True
            if device_name not in self.devices:
                any_missing = True
            if property_name is constants.ALL and len(self.devices[device_name]) == 0:
                any_missing = True
            if property_name is not constants.ALL and self.devices[device_name].get(property_name) is None:
                any_missing = True
        return any_missing

    def wait_to_connect(self, timeout_sec=None, wait_sleep_sec=0.1):
        """Wait for the connection to become ready. (Note that it's still possible
        for this function to return and the connection to be closed or broken, if
        that should happen immediately after connecting.)

        Parameters
        ----------
        *args
            see `get_properties`
        timeout_sec : Optional[float]
            Maximum timeout (+/- `wait_sleep_sec`), or None (default)
            for indefinite wait
        wait_sleep_sec : float
            Duration of wait between iterations of checking
            for readiness
        """
        start = time.time()

        while self.status is not constants.ConnectionStatus.CONNECTED:
            if timeout_sec is not None and time.time() - start >= timeout_sec:
                raise TimeoutError(f"Timed out after {timeout_sec} sec waiting for connection to become ready: {self.connection}")
            time.sleep(wait_sleep_sec)

    def get_properties_and_wait(self, *args, timeout_sec=5.0, wait_sleep_sec=0.1):
        """After subscribing to properties, wait up to `timeout_sec` for
        the properties to become available. If the timeout expires
        and the property definitions haven't been received,
        this raises a `TimeoutError`.

        Parameters
        ----------
        *args
            see `get_properties`
        timeout_sec : float
            Maximum timeout (+/- `wait_sleep_sec`)
        wait_sleep_sec : float
            Duration of wait between iterations of checking
            for readiness

        **Note:** For catch-all requests (i.e. all devices, or all
        properties for a given device), this waits until **one**
        property definition has been received. This is because there is
        no way to know if a device will send more property definitions
        later.
        """
        self.get_properties(*args)
        start = time.time()

        while time.time() - start < timeout_sec and self.interested_properties_missing:
            time.sleep(wait_sleep_sec)

        if not self.interested_properties_missing:
            return

        property_keys = []
        for device_name, property_name in self._interested_properties:
            if device_name in self.devices.names and property_name in self.devices[device_name]:
                continue
            if device_name is constants.ALL:
                device_part = '*'
            else:
                device_part = device_name
            if property_name is constants.ALL:
                property_part = '*'
            else:
                property_part = property_name
            property_keys.append(f"{device_part}.{property_part}")
        raise TimeoutError(f"Timed out after {timeout_sec} sec waiting for these properties: {property_keys}")

    def handle_connectionstatus_change(self, connection_status : constants.ConnectionStatus):
        '''When reconnecting, issue a <getProperties> message for all
        properties we have previously registered interest for.
        '''
        if self._has_connected_once and connection_status.CONNECTED:
            log.debug("Re-connecting and requesting all the same properties")
            if (constants.ALL, constants.ALL) in self._interested_properties:
                msg = messages.GetProperties()
                self.connection.send(msg)
                log.debug("Re-connected and issued catch-all getProperties")
            else:
                for device_name, prop_name in self._interested_properties:
                    self.connection.send(messages.GetProperties(
                        device=device_name,
                        name=prop_name if prop_name is not constants.ALL else None,
                    ))
                    log.debug(f"Re-connected and issued getProperties for {device_name}.{prop_name}")
        elif connection_status.CONNECTED:
            self._has_connected_once = True
            log.debug("Client connected for the first time")

    def connect(self, host: str=constants.DEFAULT_HOST, port: int=constants.DEFAULT_PORT):
        '''Creates a TCP connection on `host`:`port` and starts handling incoming messages'''
        # reset warning in case this is not the first time we're connecting
        self.last_get_properties_scope = None
        if self.connection is None:
            self.connection = transports.IndiTcpClientConnection(host=host, port=port)
        self.connection.add_callback(constants.TransportEvent.inbound, self.handle_message)
        self.connection.add_callback(constants.TransportEvent.connection, self.handle_connectionstatus_change)
        self.connection.start()

    def _register_interest(self, device_name : str, property_name : str):
        log.debug(f"Registering interest in {device_name=} {property_name=}")
        self._interested_properties.add((device_name, property_name))

    def _add_property(self, prop : properties.IndiProperty):
        self.devices[prop.device][prop.name] = prop
        log.debug(f"Added {prop} to properties proxy")

    def dispatch_callbacks(self, message : messages.IndiDefSetDelMessage):
        '''Loop through the nested dict of `self.callbacks` and compare
        message.device and message.name to the callback keys. If matched,
        call the corresponding callback with a `messages.IndiDefSetDelMessage`
        '''
        with self.callbacks_lock:
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

    def register_callback(self,
                          cb : typing.Callable[[messages.IndiDefSetDelMessage], typing.Any],
                          device_name=constants.ALL, property_name=constants.ALL):
        '''Register a function `cb(message: IndiDefSetDelMessage)` to be called
        when new INDI messages update properties in this client. This can optionally
        be scoped by `device_name` and `property_name`, otherwise it is called for all
        messages
        '''
        with self.callbacks_lock:
            self.callbacks[device_name][property_name].append(cb)
        log.debug(f"Registered callback {cb=} for {device_name=} {property_name=}")

    def unregister_callback(self,
                            cb : typing.Callable[[messages.IndiDefSetDelMessage], typing.Any],
                            device_name=constants.ALL, property_name=constants.ALL):
        '''Remove a callback function from the set of callbacks for a
        `(device_name, property_name)` pair'''
        self.callbacks[device_name][property_name].remove(cb)
        log.debug(f"Unregistered callback {cb=} for {device_name=} {property_name=}")

    def handle_message(self, message : messages.IndiDefSetDelMessage):
        """Handles property definition, updates, and deletion for all devices.
        If the incoming message is one of the `messages.IndiDefSetDelMessage`
        types, the corresponding properties are created/updated/deleted as
        needed and `dispatch_callbacks` is called with the message.
        """
        if not isinstance(message, typing.get_args(messages.IndiDefSetDelMessage)):
            return
        if isinstance(message, messages.DelProperty):
            if message.device is None:
                devices = self.devices.names
                log.debug(f"Deleting all properties for {list(devices)}")
                for k in devices:
                    del self.devices[k]
            else:
                propnames = tuple(self.devices[message.device].keys())
                for propname in propnames:
                    if message.name is None or propname == message.name:
                        try:
                            del self.devices[message.device][propname]
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
            if device_name not in self.devices or property_name not in self.devices[device_name]:
                if isinstance(message, typing.get_args(messages.IndiDefMessage)):
                    if device_name not in self.devices:
                        self.devices[device_name] = RemoteDevice(device_name)
                    self.devices[device_name][property_name] = properties.IndiProperty.from_definition(message)
                    log.debug(f"Constructed new property {self.devices[device_name][property_name]} from definition")
            else:
                self.devices[message.device][message.name].apply_update(message)
        self.dispatch_callbacks(message)

    def __getitem__(self, key):
        parts = key.split('.', 2)
        device_name = parts[0]
        if len(self._interested_properties) == 0:
            error_suffix = ", not currently listening for any. Perhaps you need to call get_properties()?"
        else:
            error_suffix = ", currently listening for: " + str(self._interested_properties)
        if device_name not in self.devices:
            raise KeyError(f"No device {device_name} represented within these properties" + error_suffix)
        device_props = self.devices[device_name]
        if len(parts) > 1:
            property_name = parts[1]
            if property_name not in device_props:
                raise KeyError(f"No property {device_name}.{property_name} represented within these properties" + error_suffix)
            prop = device_props[property_name]
            if len(parts) > 2:
                element_name = parts[2]
                if element_name not in prop:
                    raise KeyError(f"No element {device_name}.{property_name}.{element_name} represented within these properties" + error_suffix)
                return prop[element_name]
            else:
                return prop
        else:
            return {name: prop for name, prop in device_props.items()}

    def __setitem__(self, key, value):
        parts = key.split('.', 2)
        device_name = parts[0]
        if len(self._interested_properties) == 0:
            error_suffix = ", not currently listening for any. Perhaps you need to call get_properties()?"
        else:
            error_suffix = ", currently listening for: " + str(self._interested_properties)
        if device_name not in self.devices:
            raise KeyError(f"No device {device_name} represented within these properties" + error_suffix)
        device_props = self.devices[device_name]
        if len(parts) > 1:
            property_name = parts[1]
            if property_name not in device_props:
                raise KeyError(f"No property {device_name}.{property_name} represented within these properties" + error_suffix)
            prop = device_props[property_name]
            if len(parts) > 2:
                element_name = parts[2]
                if element_name not in prop:
                    raise KeyError(f"No element {device_name}.{property_name}.{element_name} represented within these properties" + error_suffix)
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
