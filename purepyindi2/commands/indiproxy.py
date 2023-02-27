import IPython
from .. import server
import logging
logging.basicConfig(level="ERROR")

def main():
    logging.getLogger('purepyindi2').setLevel('DEBUG')
    s = server.IndiServer(
        bind_host='localhost',
        bind_port=7724,
        remote_servers=[
            ('localhost', 7624),
        ]
    )
    s.start()
    IPython.embed()

if __name__ == "__main__":
    main()