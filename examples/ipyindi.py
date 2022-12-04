import IPython
from purepyindi2 import client
import logging
logging.basicConfig(level="ERROR")
# log = logging.getLogger(__name__)
# logging.getLogger('purepyindi2').setLevel('DEBUG')
# log.setLevel('DEBUG')

def main():
    c = client.IndiClient()
    c.connect()
    c.get_properties()
    IPython.embed()

if __name__ == "__main__":
    main()