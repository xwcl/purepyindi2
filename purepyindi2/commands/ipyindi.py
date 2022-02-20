import IPython
from .. import client
import logging
logging.basicConfig(level="ERROR")
logging.getLogger('purepyindi2').setLevel('DEBUG')

def main():
    c = client.IndiClient()
    c.connect()
    IPython.embed()

if __name__ == "__main__":
    main()