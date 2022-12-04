import sys
import time
from .. import client
import logging
logging.basicConfig(level="ERROR")

def main():
    c = client.IndiClient()
    c.connect()
    c.get_properties()
    time.sleep(1)
    start = time.perf_counter()
    state_bytes = c.to_json()
    duration = time.perf_counter() - start
    sys.stdout.write(state_bytes.decode('utf8'))
    sys.stderr.write(f"Took {duration * 1000} ms to serialize\n")

if __name__ == "__main__":
    main()