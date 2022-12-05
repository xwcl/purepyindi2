import logging
from purepyindi2 import device, properties
from purepyindi2.messages import DefNumber

log = logging.getLogger(__name__)

class MyDevice(device.XDevice):
    def handle_prop1(self, existing_property, new_message):
        existing_property['elem1'] = new_message['elem1']
        self.update_property(existing_property)
        
    def setup(self):
        nv = properties.NumberVector(name='prop1')
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
        else:
            prop['elem1'].value = 5
        self.update_property(prop)


logging.basicConfig(level=logging.DEBUG)
MyDevice(name="foo", fifos_root="/tmp").main()