import typing
import IPython
from purepyindi2 import client, messages
import logging
logging.basicConfig(level="ERROR")
logging.getLogger('purepyindi2').setLevel('DEBUG')

def print_updates(msg):
    if isinstance(msg, typing.get_args(messages.IndiDefSetMessage)):
        for element in msg:
            print(f"{msg.device}.{msg.name}.{element}={msg.get()}")

def main():
    c = client.IndiClient()
    c.connect()
    c.get_properties("observers.current_observer")
    IPython.embed()

if __name__ == "__main__":
    main()