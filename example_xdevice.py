#!/usr/bin/env python
import logging
from purepyindi2 import device, properties, constants
from purepyindi2.messages import DefNumber, DefSwitch

log = logging.getLogger(__name__)

class ExampleDevice(device.XDevice):
    def handle_toggle(self, existing_property, new_message):
        existing_property['toggle'].value = new_message['toggle'].value
        self.update_property(existing_property)
        log.debug(f"Handled toggle to {existing_property['toggle']}")

    def handle_switch(self, switches_turned_on : set[str], switches_turned_off: set[str]) -> bool:
        self.log.debug(f"{switches_turned_on=} {switches_turned_off=}")
        if 'third' in switches_turned_on:
            # always fail to apply changes when this switch is requested
            return False
        else:
            # succeed otherwise
            return True

    def setup(self):
        sv = properties.SwitchVector(
            name="toggle",
            rule=constants.SwitchRule.ANY_OF_MANY,
            perm=constants.PropertyPerm.READ_WRITE,
        )
        sv.add_element(DefSwitch(name="toggle", _value=constants.SwitchState.OFF))
        self.add_property(sv, callback=self.handle_toggle)
        log.debug(f"{sv}")
        
        sv = properties.SwitchVector(
            name="one_of_many",
            rule=constants.SwitchRule.ONE_OF_MANY,
            perm=constants.PropertyPerm.READ_WRITE,
        )
        sv.add_element(DefSwitch(name="first", _value=constants.SwitchState.ON))
        sv.add_element(DefSwitch(name="second", _value=constants.SwitchState.OFF))
        sv.add_element(DefSwitch(name="third", _value=constants.SwitchState.OFF))
        self.add_property(sv, callback=sv.switch_callback(self.handle_switch, self))
        log.debug(f"{sv}")

        nv = properties.NumberVector(name='uptime')
        nv.add_element(DefNumber(
            name='uptime_sec', label='Uptime', format='%3.1f',
            min=0, max=1_000_000, step=1, _value=0.0
        ))
        self.add_property(nv)
        log.debug("Set up complete")

    def loop(self):
        uptime_prop = self.properties['uptime']
        uptime_prop['uptime_sec'] += 1
        self.update_property(uptime_prop)
        log.debug(f"Current uptime: {uptime_prop}")

logging.basicConfig(level=logging.DEBUG)
ExampleDevice(name="purepyindi_example").main()
