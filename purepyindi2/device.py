import time
import datetime
import sys
import os
import psutil
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

    def _wrap_loop(self):
        '''Allow subclasses to customize behavior before and after calling `loop`'''
        self.loop()

    def run(self):
        self.client = client.IndiClient()
        self.setup()
        self.send_all_properties()
        self._setup_complete = True
        while self.connection.status is constants.ConnectionStatus.CONNECTED:
            self._wrap_loop()
            time.sleep(self.sleep_interval_sec)

    def loop(self):
        log.debug("device %s: placeholder loop logic", self.name)


class XDevice(Device):
    prefix_dir : str  = "/opt/MagAOX"
    logs_dir : str = "log"
    log : logging.Logger

    def _init_logs(self, verbose, all_verbose):
        self.log = logging.getLogger(self.name)
        log_dir = self.prefix + "/" + self.logs_dir + "/" + self.name + "/"
        os.makedirs(log_dir, exist_ok=True)
        self.log.debug(f"Made (or found) {log_dir=}")
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S")
        log_file_path = log_dir + "/" + f"{self.name}_{timestamp}.log"
        log_format = '%(filename)s:%(lineno)d: [%(levelname)s] %(message)s'
        logging.basicConfig(
            level='INFO',
            filename=log_file_path,
            format=log_format
        )
        if verbose:
            self.log.setLevel(logging.DEBUG)
        if all_verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        # Specifying a filename results in no console output, so add it back
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        logging.getLogger('').addHandler(console)
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)
        self.log.info(f"Logging to {log_file_path}")

    def __init__(self, name, *args, verbose=False, all_verbose=False, **kwargs):
        self._init_logs()
        fifos_root = self.prefix_dir + "/drivers/fifos"
        super().__init__(name, *args, connection_class=partial(transports.IndiFifoConnection, name=name, fifos_root=fifos_root), **kwargs)

    def lock_pid_file(self):
        pid_dir = self.magaox_root + "/sys/{self.name}"
        os.makedirs(pid_dir, exist_ok=True)
        pid_file = pid_dir + "/pid"
        if os.path.exists(pid_file):
            with open(pid_file) as fh:
                try:
                    pid = int(fh.read())
                    log.debug(f"Got {pid=} from {pid_file}")
                except Exception:
                    pid = None

        if pid is not None:
            if psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                with proc.oneshot():
                    if proc.exe() == sys.executable and self.name in sys.argv:
                        log.error(f"Found process ID {pid}: {proc.cmdline()} [{proc.status()}]")
                        sys.exit(1)
            else:
                log.debug("Removing stale pid file {pid_file}")
                os.remove(pid_file)
        thisproc = psutil.Process()
        with open(pid_file, 'w') as fh:
            fh.write(thisproc.pid)

    def main(self):
        self.lock_pid_file()
        super().main()

    @classmethod
    def entrypoint(cls):
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('-n', '--name', help="Device name for INDI")
        parser.add_argument('-v', '--verbose', help="Set device log level to DEBUG")
        parser.add_argument('-a', '--all-verbose', help="Set global log level to DEBUG")
        args = parser.parse_args()
        cls(name=args.name, verbose=args.verbose, all_verbose=args.all_verbose).main()