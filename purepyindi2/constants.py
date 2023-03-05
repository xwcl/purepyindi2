from enum import Enum
from typing import Union

__all__ = (
    'ConnectionStatus',
    'PropertyState',
    'PropertyPerm',
    'SwitchState',
    'SwitchRule',
    'AnyIndiValue',
    'parse_string_into_any_indi_value',
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

def parse_string_into_any_indi_value(string):
    '''Tries to turn `string` into a PropertyState (light), SwitchState, or floating-point number;
    falling back to returning the input string.
    '''
    try:
        return parse_string_into_enum(string, PropertyState)
    except ValueError:
        pass
    try:
        return parse_string_into_enum(string, SwitchState)
    except ValueError:
        pass
    try:
        return float(string)
    except ValueError:
        pass
    return string

class Role(Enum):
    DEVICE = 'device'
    CLIENT = 'client'

class TransportEvent(Enum):
    connection = 'connection'
    disconnection = 'disconnection'
    inbound = 'inbound'
    outbound = 'outbound'

class ConnectionStatus(Enum):
    CONNECTING = 'connecting'
    CONNECTED = 'connected'
    STOPPED = 'stopped'
    ERROR = 'error'
    NOT_CONFIGURED = 'not configured'

class PropertyState(Enum):
    IDLE = 'Idle'
    OK = 'Ok'
    BUSY = 'Busy'
    ALERT = 'Alert'

class PropertyPerm(Enum):
    READ_ONLY = 'ro'
    WRITE_ONLY = 'wo'
    READ_WRITE = 'rw'


class PropertyKind(Enum):
    NUMBER = 'num'
    TEXT = 'txt'
    SWITCH = 'swt'
    LIGHT = 'lgt'

class SwitchState(Enum):
    OFF = 'Off'
    ON = 'On'
    def __str__(self):
        return self.value

class SwitchRule(Enum):
    ONE_OF_MANY = 'OneOfMany'
    AT_MOST_ONE = 'AtMostOne'
    ANY_OF_MANY = 'AnyOfMany'

AnyIndiValue = Union[PropertyState, SwitchState, float, int, str]