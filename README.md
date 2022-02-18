```python
c = Client()
c.start()
snoops = ['*']
props = c.get_properties()

try:
    value = props['foo.bar.baz']
except NotFound:
    pass

snoops = ['']
try:
    props = c.get_properties(snoops, device=None, prop=None, timeout_sec=1)
except PropertyNotFound as e:
    raise

try:
    props = c.get_properties(subscriptions=['device1.foo', 'device2.foo'], timeout_sec=1)
except PropertyNotFound as e:
    raise

x = props['device1.foo']
msg = x.set(bar=2)
c.send(msg)


def setup(self):
    other_props = ['otherdevice.prop1', 'otherdevice2.prop2']
    self.client.get_properties(other_props)
    try:
        self.client.expect_properties(other_props)
    except MissingProperty as e:
        log.debug(f"Missing property requesting {other_props}: {e}")
    
    

def loop(self):
    # Update own properties
    self.properties['propname']['elem1name'] = SwitchState.ON
    self.properties['propname']['elem2name'] = SwitchState.OFF
    self.properties['propname'].state = PropertyState.BUSY
    self.send_property(self.properties['propname'])

    # Act as a client reading and writing other properties
    with self.client['otherdevice.prop1'] as prop:
        the_value = prop['elem1']
        prop['elem1'] = the_value + 1

    self.client.register_
```