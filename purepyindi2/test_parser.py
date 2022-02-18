from io import BytesIO
import pytest
import datetime
from queue import Queue
from .constants import (
    PropertyPerm,
    PropertyState,
)
from . import messages
from .parser import IndiStreamParser

TESTING_TIMESTAMP = datetime.datetime(
    2019, 8, 13, 22, 45, 17, 867692, tzinfo=datetime.timezone.utc
)

DEF_NUMBER_PROP = b"""
<defNumberVector device="test" name="prop" state="Idle" perm="rw" timestamp="2019-08-13T22:45:17.867692Z">
	<defNumber name="value" format="%g" min="0" max="0" step="0">
0
	</defNumber>
</defNumberVector>
"""

DEF_NUMBER_UPDATE = messages.DefNumberVector(
    device="test",
    name="prop",
    _elements={"value":
        messages.DefNumber(
            name="value",
            format="%g",
            max=0.0,
            min=0.0,
            step=0.0,
            _value=0.0,
        )
    },
    perm=PropertyPerm.READ_WRITE,
    state=PropertyState.IDLE,
    timestamp=TESTING_TIMESTAMP,
)

SET_NUMBER_PROP = b"""
<setNumberVector device="test" name="prop" state="Idle" timestamp="2019-08-13T22:45:17.867692Z">
	<oneNumber name="value">
1
	</oneNumber>
</setNumberVector>
"""

SET_NUMBER_UPDATE = messages.SetNumberVector(
    device="test",
    name="prop",
    state=PropertyState.IDLE,
    timestamp=TESTING_TIMESTAMP,
    _elements={"value": messages.OneNumber(name="value", _value=1.0)},
)

NEW_NUMBER_MESSAGE = b"""<newNumberVector device="test" name="prop" timestamp="2019-08-13T22:45:17.867692Z"><oneNumber name="value">0.0</oneNumber></newNumberVector>\n"""

NEW_NUMBER_UPDATE = messages.NewNumberVector(
    device="test",
    name="prop",
    _elements={"value": messages.OneNumber(name="value", _value=0.0)},
    timestamp=TESTING_TIMESTAMP,
)

DEL_PROPERTY_MESSAGE = b'''<delProperty device="test" timestamp="2019-08-13T22:45:17.867692Z">\r\n</delProperty>'''

DEL_PROPERTY_UPDATE = messages.DelProperty(
    device="test",
    timestamp=TESTING_TIMESTAMP,
)

@pytest.fixture
def myq():
    return Queue()

@pytest.fixture
def parser(myq):
    return IndiStreamParser(myq)

def test_def_number_update(myq, parser):
    input_buffer = BytesIO(DEF_NUMBER_PROP)
    data = input_buffer.read(len(DEF_NUMBER_PROP))
    parser.parse(data)
    def_update_payload = myq.get()
    assert def_update_payload == DEF_NUMBER_UPDATE

def test_def_number_roundtrip(myq, parser):
    out = DEF_NUMBER_UPDATE.to_xml_str()
    outbytes = out.encode('utf8')
    parser.parse(outbytes)
    def_update_payload = myq.get()
    assert def_update_payload == DEF_NUMBER_UPDATE

def test_set_number_update(myq, parser):
    input_buffer = BytesIO(SET_NUMBER_PROP)
    data = input_buffer.read(len(SET_NUMBER_PROP))
    parser.parse(data)
    set_update_payload = myq.get()
    assert set_update_payload == SET_NUMBER_UPDATE

@pytest.mark.parametrize('msg_instance', [DEF_NUMBER_UPDATE, SET_NUMBER_UPDATE, NEW_NUMBER_UPDATE, DEL_PROPERTY_UPDATE])
def test_roundtrip(msg_instance, myq, parser):
    outbytes = msg_instance.to_xml_bytes()
    print(outbytes)
    parser.parse(outbytes)
    payload = myq.get()
    print("payload", payload)
    assert payload == msg_instance
