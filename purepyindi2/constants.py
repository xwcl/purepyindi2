from enum import Enum

__all__ = (
    'ConnectionStatus',
    'PropertyState',
    'PropertyPerm',
    'SwitchState',
    'SwitchRule',
    'parse_string_into_enum',
    'INDI_PROTOCOL_VERSION_STRING',
    'ISO_TIMESTAMP_FORMAT',
    'DEFAULT_HOST',
    'DEFAULT_PORT',
    'CHUNK_MAX_READ_SIZE',
    'BLOCK_TIMEOUT_SEC',
    'RECONNECTION_DELAY_SEC',
    'ALL',
)

class ALL:
    pass

CHUNK_MAX_READ_SIZE = 1024
BLOCK_TIMEOUT_SEC = 1
RECONNECTION_DELAY_SEC = 2

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 7624

INDI_PROTOCOL_VERSION_STRING = '1.7'
ISO_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

def parse_string_into_enum(string, enumtype):
    for entry in enumtype:
        if string == entry.value:
            return entry
    raise ValueError(f"No enum instance in {enumtype} for string {repr(string)}")

class Role(Enum):
    DEVICE = 'device'
    CLIENT = 'client'

class TransportEvent(Enum):
    connection = 'connection'
    inbound = 'inbound'
    outbound = 'outbound'

class ConnectionStatus(Enum):
    STARTING = 'starting'
    CONNECTED = 'connected'
    RECONNECTING = 'reconnecting'
    STOPPED = 'stopped'
    ERROR = 'error'

class PropertyState(Enum):
    IDLE = 'Idle'
    OK = 'Ok'
    BUSY = 'Busy'
    ALERT = 'Alert'

class PropertyPerm(Enum):
    READ_ONLY = 'ro'
    WRITE_ONLY = 'wo'
    READ_WRITE = 'rw'

class SwitchState(Enum):
    OFF = 'Off'
    ON = 'On'
    def __str__(self):
        return self.value

class SwitchRule(Enum):
    ONE_OF_MANY = 'OneOfMany'
    AT_MOST_ONE = 'AtMostOne'
    ANY_OF_MANY = 'AnyOfMany'
