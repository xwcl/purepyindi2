import socket
from purepyindi2.messages import Message
from purepyindi2.parser import IndiStreamParser
conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn.connect(("localhost", 7624))
conn.settimeout(10)
conn.sendall(b'<getProperties version="1.7" />\n\n')
import queue
q = queue.Queue()
parser = IndiStreamParser(q)

while True:
    try:
        data = conn.recv(1024)
        parser.parse(data)
        try:
            while True:
                msg = q.get_nowait()
                print(msg)
        except queue.Empty:
            pass
    except socket.timeout:
        continue
    # print(data.decode('utf8'))