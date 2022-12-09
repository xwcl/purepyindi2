import logging
from purepyindi2 import device, properties
from purepyindi2.messages import DefNumber

log = logging.getLogger(__name__)

class MyDevice(device.Device):
    def handle_prop1(self, existing_property, new_message):
        existing_property['elem1'].value = new_message['elem1'].value
        self.update_property(existing_property)

    def setup(self):
        nv = properties.NumberVector(name='prop1')
        print(nv.to_xml_str())
        nv.add_element(DefNumber(
            name='elem1', label='Element 1', format='%3.1f',
            min=0, max=10, step=0.1, _value=0.0
        ))
        self.add_property(nv, callback=self.handle_prop1)
        log.debug("Set up complete")

    def loop(self):
        prop = self.properties['prop1']
        if prop['elem1'].value == 5:
            prop['elem1'].value = 0
            print('set zero')
        else:
            prop['elem1'].value = 5
            print('set 5')
        log.debug(f"Switching elem1 {prop=}")
        self.update_property(prop)


logging.basicConfig(level=logging.DEBUG)
MyDevice(name="purepyindi_example").main()