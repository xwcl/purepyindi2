
from collections import defaultdict
import typing
import logging
from .vendor import dataclasses
from .messages import * # IndiDefSetDelMessage, DelProperty, IndiDefMessage, IndiSetMessage
from .constants import PropertyState, Role, SwitchRule, PropertyPerm

log = logging.getLogger(__name__)

class NoSuchElementException(Exception):
    pass

@dataclasses.dataclass(kw_only=True)
class IndiProperty:
    device : typing.Optional[str] = None  # omitted when used on device side
    perm : typing.Optional[PropertyPerm] = PropertyPerm.READ_ONLY
    _role : Role = Role.CLIENT
    MESSAGE_NEW : typing.ClassVar
    MESSAGE_SET : typing.ClassVar
    ELEMENT_CLASS : typing.ClassVar

    @classmethod
    def tag(cls):
        return 'def' + cls.__name__

    @staticmethod
    def from_definition(message):
        for cls in (DefTextVector, DefNumberVector, DefSwitchVector, DefLightVector):
            if isinstance(message, cls):
                newcls = DEF_TO_PROPERTY[cls]
                flds = dataclasses.fields(cls)
                kwargs = {}
                for fld in flds:
                    kwargs[fld.name] = getattr(message, fld.name)
                return newcls(**kwargs)
        raise TypeError("Can only construct IndiProperty subclasses given Def*Vector instances")

    def to_serializable(self):
        return dataclasses.asdict(self)

    def _construct_outbound_message(self) -> typing.Union[IndiNewMessage, IndiSetMessage]:
        if self._role is Role.CLIENT:
            cls = self.MESSAGE_NEW
        elif self._role is Role.DEVICE:
            cls = self.MESSAGE_SET
        flds = dataclasses.fields(cls)
        kwargs = {}
        for fld in flds:
            if fld.name[0] == '_':
                continue
            kwargs[fld.name] = getattr(self, fld.name)
        return cls(**kwargs)

    def make_set_property(self) -> IndiSetMessage:
        msg = self._construct_outbound_message()
        for element_name in self._elements:
            elem = self._elements[element_name]
            value = elem._value
            if not elem.validate(value):
                raise ValueError(f"Invalid value {repr(value)} for {element_name} in property {self.name}")
            msg._elements[element_name] = msg.ELEMENT_CLASS(name=element_name, _value=value)
        return msg

    def make_new_property(self, **kwargs) -> IndiNewMessage:
        msg = self._construct_outbound_message()
        for element_name in kwargs:
            if element_name not in self._elements:
                raise ValueError(f"No element named {repr(element_name)} in property {self.name}")
            elem = self._elements[element_name]
            value = kwargs[element_name]
            if not elem.validate(value):
                raise ValueError(f"Invalid value {repr(value)} for {element_name} in property {self.name}")
            # > The Client must send all members of Number and Text
            # > vectors, or may send just the members that change
            # > for other types.
            #    - INDI Whitepaper, page 4
            # "You know, it's fine to have our own standard"
            #    - Dr. Jared R. Males, 2019-11-11
            msg._elements[element_name] = msg.ELEMENT_CLASS(name=element_name, _value=value)

        return msg

    def __getitem__(self, key):
        try:
            elem = self._elements[key]
        except KeyError:
            raise NoSuchElementException(f"No element {repr(key)} in {self}")
        return elem

    def __setitem__(self, key, value):
        self._elements[key].validate(value)
        self._elements[key]._value = value

@dataclasses.dataclass(kw_only=True)
class NumberVector(IndiProperty, DefNumberVector):
    MESSAGE_NEW : typing.ClassVar = NewNumberVector
    MESSAGE_SET : typing.ClassVar = SetNumberVector

@dataclasses.dataclass(kw_only=True)
class TextVector(IndiProperty, DefTextVector):
    MESSAGE_NEW : typing.ClassVar = NewTextVector
    MESSAGE_SET : typing.ClassVar = SetTextVector

@dataclasses.dataclass(kw_only=True)
class LightVector(IndiProperty, DefLightVector):
    MESSAGE_NEW : typing.ClassVar = None
    MESSAGE_SET : typing.ClassVar = SetLightVector

@dataclasses.dataclass(kw_only=True)
class SwitchVector(IndiProperty, DefSwitchVector):
    MESSAGE_NEW : typing.ClassVar = NewSwitchVector
    MESSAGE_SET : typing.ClassVar = SetSwitchVector

DEF_TO_PROPERTY = {
    DefTextVector: TextVector,
    DefNumberVector: NumberVector,
    DefSwitchVector: SwitchVector,
    DefLightVector: LightVector,
}