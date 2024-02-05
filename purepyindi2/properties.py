from __future__ import annotations
from functools import wraps
import typing
import logging
from .vendor import dataclasses
from .messages import * # IndiDefSetDelMessage, DelProperty, IndiDefMessage, IndiSetMessage
from .constants import Role, SwitchRule, PropertyPerm, SwitchState

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

    def switch_callback(self, apply_changes: typing.Callable[[set[str], set[str]], bool], device: 'Device'):
        """Wrapper for callback functions that enforces the SwitchRule
        in self.rule and provides "transactional"/"rollback" semantics
        to a switch change.

        Usage
        -----

        To use, wrap your callback with a call to this method and
        pass in the device instance.

        >>> sv = SwitchVector(name="any_of_many", rule=SwitchRule.ANY_OF_MANY, perm=PropertyPerm.READ_WRITE)
        >>> sv.add_element(DefSwitch(name="toggle", _value=constants.SwitchState.OFF))
        >>> def handle_toggle(switches_turned_on : set[str], switches_turned_off: set[str]) -> bool:
        ...     print(f"{switches_turned_on=} {switches_turned_off=}")
        ...     return True
        ...
        >>> my_device.add_property(sv, callback=sv.switch_callback(handle_toggle, my_device))

        The wrapper validates that the message is valid using the
        rule in `self.rule`. The callback then receives two arguments,
        `switches_turned_on` and `switches_turned_off`. These are sets
        with the element names of switches turned on or off by the
        incoming message.

        If the device code successfully applies the new state, the
        callback should return `True`. The decorator then takes care of
        housekeeping like turning off all but the requested switch for
        the OneOfMany rule and sending an update with the changed
        property.

        The user code can assume that the elements named in
        `switches_turned_on` are currently Off, and the incoming
        message is attempting to toggle them on. Likewise, names in
        `switches_turned_off` were On and the incoming message will set
        them to Off.

        User code can assume the SwitchRule was applied to validate the
        incoming message. In detail:

        * If the incoming message attempts to set multiple switch
        elements to On for a `OneOfMany` or `AtMostOne` SwitchVector,
        the message is rejected and the user callback is not called.
        * If the incoming message attempts to set the currently On
        element to Off for a `OneOfMany` SwitchVector, the message is
        rejected and the user callback is not called.
        * If the incoming message does not change the state of the
        SwitchVector, e.g. by setting things to Off that were already
        Off, the user callback is not called.
        """
        @wraps(apply_changes)
        def wrapper(existing_property : SwitchVector, new_message : NewSwitchVector):
            switches_on = set()
            switches_turned_on = set()
            switches_turned_off = set()
            all_switches = set()
            for swname in existing_property:
                all_switches.add(swname)
                # Collect existing state
                if existing_property[swname] is SwitchState.ON:
                    switches_on.add(swname)
                # Collect elements that are changing state
                if existing_property[swname] != new_message[swname]:
                    if new_message[swname] is SwitchState.ON:
                        switches_turned_on.add(swname)
                    elif new_message[swname] is SwitchState.OFF:
                        switches_turned_off.add(swname)

            if len(switches_turned_on) > 0 or len(switches_turned_off) > 0:
                if self.rule is SwitchRule.ONE_OF_MANY:
                    if (switches_turned_off and not switches_turned_on) or len(switches_turned_on) > 1:
                        # re-assert existing
                        device.update_property(existing_property)
                        return
                    # Even if the newSwitchVector message does not contain all elements of the switch
                    # we want to do something sensible. This ensures anything that was On is correctly
                    # flagged as turning off, whether or not it was supplied with Off in the message.
                    switches_turned_off = switches_on - switches_turned_on
                elif self.rule is SwitchRule.AT_MOST_ONE:
                    if len(switches_turned_on) > 1:
                        # re-assert existing
                        device.update_property(existing_property)
                        return
                    # Even if the newSwitchVector message does not contain all elements of the switch
                    # we want to do something sensible. This ensures anything that was On is correctly
                    # flagged as turning off, whether or not it was supplied with Off in the message.
                    switches_turned_off = switches_on - switches_turned_on
                elif self.rule is SwitchRule.ANY_OF_MANY:
                    # Only call the callback if something has changed
                    if not (len(switches_turned_on) > 0 or len(switches_turned_off) > 0):
                        # re-assert existing
                        device.update_property(existing_property)
                        return
                success = apply_changes(switches_turned_on, switches_turned_off)

                if success:
                    # only apply updates to existing_property and enqueue an outbound update
                    # when the switching on/off was successful
                    for swname in switches_turned_off:
                        existing_property[swname] = SwitchState.OFF
                    for swname in switches_turned_on:
                        existing_property[swname] = SwitchState.ON

            device.update_property(existing_property)
        return wrapper


DEF_TO_PROPERTY = {
    DefTextVector: TextVector,
    DefNumberVector: NumberVector,
    DefSwitchVector: SwitchVector,
    DefLightVector: LightVector,
}