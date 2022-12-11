from typing import Optional, Union, get_args, ClassVar
from xml.etree import ElementTree as Etree
import warnings
from functools import partial
import datetime
from . import constants
from .vendor import dataclasses
from enum import Enum

import logging
log = logging.getLogger(__name__)

__all__ = [
    "OneText",
    "OneNumber",
    "OneSwitch",
    "OneLight",
    "DefVector",
    "DefTextVector",
    "DefText",
    "DefNumberVector",
    "DefNumber",
    "DefSwitchVector",
    "DefSwitch",
    "DefLightVector",
    "DefLight",
    "SetVector",
    "SetTextVector",
    "SetNumberVector",
    "SetSwitchVector",
    "SetLightVector",
    "Message",
    "DelProperty",
    "GetProperties",
    "NewTextVector",
    "NewNumberVector",
    "NewSwitchVector",
    "IndiMessage",
    "IndiTopLevelMessage",
    "IndiNewMessage",
    "IndiSetMessage",
    "IndiDefMessage",
    "IndiDefSetDelMessage",
    "IndiElementMessage",
    "IndiDefElementMessage",
    "format_datetime_as_iso",
]


def format_datetime_as_iso(dt):
    return dt.astimezone(datetime.timezone.utc).strftime(constants.ISO_TIMESTAMP_FORMAT)


message = partial(dataclasses.dataclass, kw_only=True)

_ATTRIBUTE_CONVERTERS = {
    "timestamp": format_datetime_as_iso,
    "state": lambda x: x.value,
    "perm": lambda x: x.value,
    "rule": lambda x: x.value,
}

class MessageBase:
    @classmethod
    def tag(cls):
        x = cls.__name__
        return x[0].lower() + x[1:]

    def to_xml_element(self) -> Etree.Element:
        this = Etree.Element(self.tag())
        flds = [x.name for x in dataclasses.fields(self) if x.name[0] != "_"]
        for attrname in flds:
            attrval = getattr(self, attrname, None)
            if attrval is not None:
                attr_converter = _ATTRIBUTE_CONVERTERS.get(attrname, str)
                this.set(attrname, attr_converter(attrval))
        return this

    def to_xml_bytes(self) -> bytes:
        return Etree.tostring(
            self.to_xml_element(), encoding="utf8", xml_declaration=False
        )

    def to_xml_str(self) -> str:
        return self.to_xml_bytes().decode("utf8")


@message
class ValueMessageBase(MessageBase):
    name: str = None
    # default None because parser instantiates subclasses before
    # chardata has been seen to set _value:
    _value: object = dataclasses.field(default=None)

    def get(self):
        return self._value

    @staticmethod
    def value_from_text(value):
        return value

    @property
    def value(self):
        return self._value

    def validate(self, value) -> bool:
        raise NotImplementedError("Subclasses must implement validate()")

    @value.setter
    def value(self, new_value):
        try:
            self.set_from_text(new_value)
        except Exception as e:
            if self.validate(new_value):
                self._value = new_value
            else:
                raise RuntimeError(f"Couldn't interpret {new_value=} as text or validated <{self.tag()}> value (original exception: {e})")

    def __post_init__(self):
        self.value = self._value  # pass it through the validating setter

    def set_from_text(self, value):
        if value is None:
            self._value = None
            return
        self._value = self.value_from_text(value)

    def to_xml_element(self):
        el = super().to_xml_element()
        if isinstance(self._value, Enum):
            text = self._value.value
        elif self._value is not None:
            text = str(self._value)
        else:
            text = ""
        el.text = text
        return el

@message
class OneText(ValueMessageBase):
    _value: str

    def validate(self, value):
        try:
            str(value)
            return True
        except Exception:
            return False

@message
class OneNumber(ValueMessageBase):
    _value: float

    @staticmethod
    def value_from_text(value):
        if value is None:
            return None
        try:
            parsed_number = float(value)
        except TypeError:
            raise ValueError(f"Unparseable number {repr(value)}")
        return parsed_number
    
    def validate(self, value) -> bool:
        if value < self.min or value > self.max:
            warnings.warn(f"Value {value} isn't {self.min} <= value <= {self.max} (bounds from property definition)")
        try:
            value = float(value)
            return True
        except Exception:
            return False

@message
class OneSwitch(ValueMessageBase):
    _value: constants.SwitchState

    @staticmethod
    def value_from_text(value):
        return constants.parse_string_into_enum(value, constants.SwitchState)

    def validate(self, value) -> bool:
        return isinstance(value, constants.SwitchState)

@message
class OneLight(ValueMessageBase):
    _value: constants.PropertyState

    @staticmethod
    def value_from_text(value):
        return constants.parse_string_into_enum(value, constants.SwitchState)

    def validate(self, value) -> bool:
        return isinstance(value, constants.PropertyState)


@message
class DefValueMessageBase(ValueMessageBase):
    label: Optional[str] = None


@message
class DefText(DefValueMessageBase, OneText):
    pass

@message
class DefNumber(DefValueMessageBase, OneNumber):
    format: str
    min: float
    max: float
    step: float


@message
class DefSwitch(DefValueMessageBase, OneSwitch):
    _value: constants.SwitchState


@message
class DefLight(DefValueMessageBase, OneLight):
    _value: constants.PropertyState

IndiDefElementMessage = Union[DefText, DefNumber, DefSwitch, DefLight]

@message
class PropertyMessageBase(MessageBase):
    device: str
    name: str
    timestamp: Optional[datetime.datetime] = None
    _elements: dict = dataclasses.field(default_factory=dict)

    def add_element(self, element):
        if element.name in self._elements:
            raise ValueError(f"Attempted redefinition of element named {element.name}")
        self._elements[element.name] = element

    def to_xml_element(self):
        el = super().to_xml_element()
        for property_element in self._elements:
            el.append(self._elements[property_element].to_xml_element())
        return el

    def elements(self):
        return self._elements.items()

    def __contains__(self, key):
        return key in self._elements

    def __getitem__(self, key):
        try:
            return self._elements[key]._value
        except KeyError:
            raise KeyError(f"No element name {key} in property")

    def __iter__(self):
        return iter(self._elements.keys())

    def apply_update(self, message):
        did_change = False
        if message.timestamp is not None:
            self.timestamp = message.timestamp
            did_change = True
        for element_name in message:
            if element_name not in self:
                if isinstance(message[element_name], IndiDefElementMessage):
                    # handle redefinition
                    self.add_element(message[element_name])
                    did_change = True
                else:
                    log.debug(f"Got element {element_name} as {message[element_name]} but haven't seen it before")
            current_value = self._elements[element_name]._value
            if current_value != message._elements[element_name]._value:
                self._elements[element_name]._value = message._elements[element_name]._value
                did_change = True
        return did_change

@message
class DefSetMessageBase(PropertyMessageBase):
    timeout: Optional[str] = None
    message: Optional[str] = None
    state: Optional[constants.PropertyState] = None
    label: Optional[str] = None
    group: Optional[str] = None

    def apply_update(self, message):
        did_change = super().apply_update(message)
        if message.timeout is not None:
            self.timeout = message.timeout
            did_change = True
        if message.state is not None:
            self.state = message.state
            did_change = True
        if message.label is not None:
            self.label = message.label
            did_change = True
        if message.group is not None:
            self.group = message.group
            did_change = True
        return did_change

@message
class DefVector(DefSetMessageBase):
    state: constants.PropertyState = constants.PropertyState.OK # not optional for def

@message
class DefSettableVector(DefVector):
    perm: constants.PropertyPerm

    def apply_update(self, message):
        did_change = super().apply_update(message)
        if hasattr(message, 'perm'):
            self.perm = message.perm
            did_change = True
        return did_change

# Define properties and initial values

@message
class DefTextVector(DefSettableVector):
    ELEMENT_CLASS : ClassVar = DefText
    _kind : constants.PropertyKind = constants.PropertyKind.TEXT


@message
class DefNumberVector(DefSettableVector):
    ELEMENT_CLASS : ClassVar = DefNumber
    _kind : constants.PropertyKind = constants.PropertyKind.NUMBER

@message
class DefSwitchVector(DefSettableVector):
    ELEMENT_CLASS : ClassVar = DefSwitch
    rule: constants.SwitchRule
    _kind : constants.PropertyKind = constants.PropertyKind.SWITCH

    def apply_update(self, message):
        did_change = super().apply_update(message)
        if hasattr(message, 'rule'):
            self.rule = message.rule
            did_change = True
        return did_change

@message
class DefLightVector(DefVector):
    ELEMENT_CLASS : ClassVar = DefLight
    _kind : constants.PropertyKind = constants.PropertyKind.LIGHT

# Updated property values from device

@message
class SetVector(DefSetMessageBase):
    # Though marked optional in superclass, these attributes
    # must not be set in set*Vector messages
    def __post_init__(self):
        if self.label != None:
            raise ValueError(f"label attribute cannot be set on {self.__class__.__name__}")
        if self.group != None:
            raise ValueError(f"label attribute cannot be set on {self.__class__.__name__}")

@message
class SetTextVector(SetVector):
    ELEMENT_CLASS : ClassVar = OneText
    _elements: dict[str,OneText] = dataclasses.field(default_factory=dict)


@message
class SetNumberVector(SetVector):
    ELEMENT_CLASS : ClassVar = OneNumber
    _elements: dict[str,OneNumber] = dataclasses.field(default_factory=dict)


@message
class SetSwitchVector(SetVector):
    ELEMENT_CLASS : ClassVar = OneSwitch
    _elements: dict[str,OneSwitch] = dataclasses.field(default_factory=dict)


@message
class SetLightVector(SetVector):
    ELEMENT_CLASS : ClassVar = OneLight
    _elements: dict[str,OneLight] = dataclasses.field(default_factory=dict)


# Device to client messages

@message
class Message(MessageBase):
    message: str
    device: Optional[str] = None
    timestamp: Optional[datetime.datetime] = None


@message
class DelProperty(MessageBase):
    device: Optional[str] = None
    name: Optional[str] = None
    timestamp: Optional[datetime.datetime] = None
    message: Optional[str] = None

# Client to device messages
@message
class GetProperties(MessageBase):
    device: Optional[str] = None
    name: Optional[str] = None
    version: str = constants.INDI_PROTOCOL_VERSION_STRING


@message
class NewTextVector(PropertyMessageBase):
    ELEMENT_CLASS : ClassVar = OneText
    _elements: dict[str,OneText] = dataclasses.field(default_factory=dict)


@message
class NewNumberVector(PropertyMessageBase):
    ELEMENT_CLASS : ClassVar = OneNumber
    _elements: dict[str,OneNumber] = dataclasses.field(default_factory=dict)


@message
class NewSwitchVector(PropertyMessageBase):
    ELEMENT_CLASS : ClassVar = OneSwitch
    _elements: dict[str,OneSwitch] = dataclasses.field(default_factory=dict)


IndiMessage = Union[
    OneText,
    OneNumber,
    OneSwitch,
    OneLight,
    DefTextVector,
    DefText,
    DefNumberVector,
    DefNumber,
    DefSwitchVector,
    DefSwitch,
    DefLightVector,
    DefLight,
    SetTextVector,
    SetNumberVector,
    SetSwitchVector,
    SetLightVector,
    Message,
    DelProperty,
    GetProperties,
    NewTextVector,
    NewNumberVector,
    NewSwitchVector,
]
IndiNumberProperty = Union[
    DefNumberVector,
    SetNumberVector,
    NewNumberVector,
]
IndiTextProperty = Union[
    DefTextVector,
    SetTextVector,
    NewTextVector,
]
IndiLightProperty = Union[
    DefLightVector,
    SetLightVector,
]
IndiSwitchProperty = Union[
    DefSwitchVector,
    SetSwitchVector,
    NewSwitchVector,
]
IndiTopLevelMessage = Union[
    DefTextVector,
    DefNumberVector,
    DefSwitchVector,
    DefLightVector,
    SetTextVector,
    SetNumberVector,
    SetSwitchVector,
    SetLightVector,
    Message,
    DelProperty,
    GetProperties,
    NewTextVector,
    NewNumberVector,
    NewSwitchVector,
]
IndiDefMessage = Union[DefTextVector, DefNumberVector, DefSwitchVector, DefLightVector]
IndiSetMessage = Union[SetTextVector, SetNumberVector, SetSwitchVector, SetLightVector]
IndiDefSetMessage = Union[
    DefTextVector, DefNumberVector, DefSwitchVector, DefLightVector, SetTextVector,
    SetNumberVector, SetSwitchVector, SetLightVector
]
IndiDefSetDelMessage = Union[
    DefTextVector,
    DefNumberVector,
    DefSwitchVector,
    DefLightVector,
    SetTextVector,
    SetNumberVector,
    SetSwitchVector,
    SetLightVector,
    DelProperty,
]
IndiNewMessage = Union[NewTextVector, NewNumberVector, NewSwitchVector]
IndiElementMessage = Union[
    OneText, OneNumber, OneSwitch, OneLight, DefText, DefNumber, DefSwitch, DefLight
]
