import IPython
import purepyindi2
from purepyindi2 import *
import logging
logging.basicConfig(level="ERROR")

def verbose():
    logging.getLogger('purepyindi2').setLevel('DEBUG')
    print("Debug logging enabled.")

def quiet():
    print("Debug logging disabled.")
    logging.getLogger('purepyindi2').setLevel('ERROR')

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--hostname', '-n', default='localhost', required=False, help='INDI server hostname/bind address')
    parser.add_argument('--port', '-p', type=int, default=7624, required=False, help='INDI server port')
    args = parser.parse_args()

    c = IndiClient()
    c.connect(host=args.hostname, port=args.port)
    print(f"Connected to {c.connection.host}:{c.connection.port}")
    intro = '''
IndiClient instance is available as `c`. Constants like ON and OFF are available.
You can start with `c.get_properties('devicename')`.
To enable debug logs, call `verbose()`. (Disable with `quiet()`.)'''

    print(intro)
    IPython.embed(display_banner=False)

if __name__ == "__main__":
    import sys
    sys.exit(main())
