import time
from io import BytesIO
from .transports import IndiPipeConnection, IndiTcpConnection
from .test_parser import NEW_NUMBER_MESSAGE, NEW_NUMBER_UPDATE

def test_pipe_transport():
    inbuf = BytesIO(NEW_NUMBER_MESSAGE)
    outbuf = BytesIO()
    conn = IndiPipeConnection(input_pipe=inbuf, output_pipe=outbuf)
    msgs = []
    def handler(msg):
        msgs.append(msg)
    conn.register_message_handler(handler)
    conn.start()
    time.sleep(0.2)
    conn.stop()
    assert len(msgs) == 1
    assert msgs[0] == NEW_NUMBER_UPDATE
