from xml.parsers import expat
import datetime
from .constants import (
    ISO_TIMESTAMP_FORMAT,
    PropertyPerm,
    PropertyState,
    SwitchRule,
    parse_string_into_enum,
)
import typing
from .messages import *
from pprint import pformat
import logging

from .messages import DefSettableVector
from .messages import IndiElementMessage

log = logging.getLogger(__name__)

try:
    import ciso8601
    _parse_datetime = ciso8601.parse_datetime
    def parse_optional_timestamp(timestamp : typing.Optional[str]) -> typing.Optional[datetime.datetime]:
        if timestamp is None:
            return None
        return ciso8601.parse_datetime(timestamp)
except ImportError:
    def parse_optional_timestamp(timestamp : typing.Optional[str]) -> typing.Optional[datetime.datetime]:
        if timestamp is None:
            return None
        dt = datetime.datetime.strptime(timestamp, ISO_TIMESTAMP_FORMAT)
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt

class IndiStreamParser:
    PROPERTY_DEF_LOOKUP = {x.tag(): x for x in typing.get_args(IndiDefMessage)}
    PROPERTY_SET_LOOKUP = {x.tag(): x for x in typing.get_args(IndiSetMessage)}
    PROPERTY_NEW_LOOKUP = {x.tag(): x for x in typing.get_args(IndiNewMessage)}
    PROPERTY_ELEMENT_LOOKUP = {x.tag(): x for x in typing.get_args(IndiElementMessage)}

    def __init__(self, update_queue):
        self.update_queue = update_queue
        self.current_indi_element : typing.Optional[IndiElementMessage] = None
        self.pending_update : typing.Optional[IndiTopLevelMessage] = None
        self.accumulated_chardata : str = ''
        self.accumulated_elements : list[IndiElementMessage] = []
        self.parser = self._new_parser()

    def _new_parser(self):
        parser = expat.ParserCreate()
        parser.StartElementHandler = self.start_xml_element_handler
        parser.EndElementHandler = self.end_xml_element_handler
        parser.CharacterDataHandler = self.character_data_handler
        parser.Parse('<indi>')  # Fool parser into thinking this is all one long XML document
        return parser

    def parse(self, data : str):
        try:
            self.parser.Parse(data)
        except expat.ExpatError as e:
            self.parser = self._new_parser()
            self.accumulated_chardata = ''
            self.pending_update = None
            self.current_indi_element = None
            log.warning(f"reset parser state after encountering bad input: {e}")

    def start_xml_element_handler(self, tag_name : str, tag_attributes : str):
        if self.accumulated_chardata.strip():
            log.debug(f'character data {repr(self.accumulated_chardata)} cannot be sibling of element, discarding')
        if tag_name in self.PROPERTY_DEF_LOOKUP:
            if self.pending_update is not None:
                log.debug(f'property definition happening while we '
                      f'thought something else was happening. '
                      f'Discarded pending update was: '
                      f'{self.pending_update}')
            cls : IndiDefMessage = self.PROPERTY_DEF_LOOKUP[tag_name]
            kwargs = dict(
                device=tag_attributes['device'],
                name=tag_attributes['name'],
                timeout=tag_attributes.get('timeout'),
                timestamp=parse_optional_timestamp(tag_attributes.get('timestamp')),
                message=tag_attributes.get('message'),
                state=parse_string_into_enum(tag_attributes['state'], PropertyState),
                label=tag_attributes.get('label'),
                group=tag_attributes.get('group'),
            )
            if issubclass(cls, DefSettableVector):
                kwargs['perm'] = parse_string_into_enum(tag_attributes['perm'], PropertyPerm)
            if issubclass(cls, DefSwitchVector):
                kwargs['rule'] = parse_string_into_enum(tag_attributes['rule'], SwitchRule)
            self.pending_update = cls(**kwargs)
        elif tag_name in self.PROPERTY_SET_LOOKUP:
            if self.pending_update is not None:
                log.debug(f'property setting happening while we thought '
                      f'something else was happening. '
                      f'Discarded pending update was: '
                      f'{self.pending_update}')
            cls : IndiSetMessage = self.PROPERTY_SET_LOOKUP[tag_name]
            state = parse_string_into_enum(tag_attributes['state'], PropertyState) if 'state' in tag_attributes else None
            kwargs = dict(
                device=tag_attributes['device'],
                name=tag_attributes['name'],
                timeout=tag_attributes.get('timeout'),
                timestamp=parse_optional_timestamp(tag_attributes.get('timestamp')),
                message=tag_attributes.get('message'),
                state=state,
            )
            self.pending_update = cls(**kwargs)
        elif tag_name in self.PROPERTY_NEW_LOOKUP:
            if self.pending_update is not None:
                log.debug(f'property new value arriving while we thought '
                      f'something else was happening. '
                      f'Discarded pending update was: '
                      f'{self.pending_update}')
            cls : IndiNewMessage = self.PROPERTY_NEW_LOOKUP[tag_name]
            self.pending_update = cls(device=tag_attributes['device'], name=tag_attributes['name'], timestamp=parse_optional_timestamp(tag_attributes.get('timestamp')))
        elif tag_name in self.PROPERTY_ELEMENT_LOOKUP:
            if self.pending_update is None:
                log.debug(f'Element definition/setting happening outside property definition/setting')
                self.current_indi_element = None
                return
            cls = self.PROPERTY_ELEMENT_LOOKUP[tag_name]
            kwargs = dict(
                name=tag_attributes['name'],
            )
            if issubclass(cls, typing.get_args(IndiDefElementMessage)):
                kwargs['label'] = tag_attributes.get('label')
            if cls is DefNumber:
                kwargs.update({
                    'format': tag_attributes['format'],
                    'min': float(tag_attributes['min']),
                    'max': float(tag_attributes['max']),
                    'step': float(tag_attributes['step']),
                })
            self.current_indi_element = cls(**kwargs)
        elif tag_name == DelProperty.tag():
            self.pending_update = DelProperty(
                device=tag_attributes.get('device'),
                name=tag_attributes.get('name'),
                timestamp=parse_optional_timestamp(tag_attributes['timestamp']),
                message=tag_attributes.get('message')
            )
        elif tag_name == GetProperties.tag():
            self.pending_update = GetProperties(
                device=tag_attributes.get('device'),
                name=tag_attributes.get('name'),
                version=tag_attributes.get('version'),
            )
        elif tag_name == "indi":
            # poked into parser by us at init so it treats the whole
            # incoming stream as one document
            pass
        else:
            log.debug(f"Unhandled tag <{tag_name}> opened")

    def end_xml_element_handler(self, tag_name):
        contents = self.accumulated_chardata.strip()
        self.accumulated_chardata = ''
        if tag_name in self.PROPERTY_ELEMENT_LOOKUP:
            element = self.current_indi_element
            if element is None:
                return
            if len(contents) == 0:
                # Notable spec deviation: Unset elements are not
                # provided for in Indi, but have their uses.
                # They are represented by `None` in the Python API.
                contents = None
            element.set_from_text(contents)
            self.pending_update.add_element(element)
            self.current_indi_element = None
        elif (
            tag_name == GetProperties.tag() or
            tag_name == DelProperty.tag() or
            tag_name in self.PROPERTY_DEF_LOOKUP or 
            tag_name in self.PROPERTY_SET_LOOKUP or 
            tag_name in self.PROPERTY_NEW_LOOKUP
        ):
            log.debug("Placing update in queue:")
            log.debug(pformat(self.pending_update))
            self.update_queue.put(self.pending_update)
            self.pending_update = None
            log.debug("Cleared pending update")
        else:
            log.debug(f"Unhandled tag <{tag_name}> closed")

    def character_data_handler(self, data):
        self.accumulated_chardata += data
