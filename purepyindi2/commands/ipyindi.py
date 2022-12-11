import IPython
from .. import client
import logging
logging.basicConfig(level="ERROR")

def main():
    c = client.IndiClient()
    c.connect()
    IPython.embed()

if __name__ == "__main__":
    main()