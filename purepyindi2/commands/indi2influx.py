import atexit
import datetime
from functools import partial
import logging
import os
import time

from influxdb_client import WriteApi
from influxdb_client.client.influxdb_client import InfluxDBClient

from purepyindi2 import client, messages

logging.basicConfig(level='ERROR')
log = logging.getLogger(__name__)

def on_exit(db_client: InfluxDBClient, write_api: WriteApi):
    write_api.close()
    db_client.close()

def relay(message : messages.IndiMessage, write_api: WriteApi, bucket: str):
    if not isinstance(message, (messages.DefNumberVector, messages.SetNumberVector)):
        return
    device_name, prop_name = message.device, message.name
    for element_name, elem in message.elements():
        metric_value = elem.get()
        if metric_value in (None, float('inf'), float('-inf')):
            continue
        if message.timestamp is not None:
            timestamp_ns = int(message.timestamp.timestamp() * 1e9)
        else:
            timestamp_ns = int(datetime.datetime.now().timestamp() * 1e9)
        # ex:
        # myMeasurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 1556813561098000000
        record = f"{prop_name},xdevice={device_name} {element_name}={metric_value} {timestamp_ns}"
        log.debug(record)
        write_api.write(bucket=bucket, record=record)

def main():
    influx_url = os.environ.get('INFLUX_URL', "http://localhost:8086")
    influx_token = os.environ['INFLUX_TOKEN']
    influx_org = os.environ.get('INFLUX_ORGANIZATION', 'magao-x')
    influx_bucket = os.environ.get('INFLUX_BUCKET', 'firehose')
    db_client = InfluxDBClient(
        url=influx_url,
        token=influx_token,
        org=influx_org,
    )
    write_api = db_client.write_api()
    atexit.register(on_exit, db_client, write_api)

    while True:
        try:
            c = client.IndiClient()
            callback = partial(relay, write_api=write_api, bucket=influx_bucket)
            c.register_callback(callback)
            c.connect()
            c.get_properties()
            log.info("Listening for metrics")
            while True:
                time.sleep(1)
        except Exception:
            log.exception("Restarting IndiClient on error...")

if __name__ == "__main__":
    main()