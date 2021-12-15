from typing import Dict
from server import *

FEEDS=['light','motor','soil','temperature','humidity']

row=dict()
def message(client, topic_id, payload:Dict):
    print(payload)
    if FEEDS[-1] not in payload.keys():
        row.update(payload)
    if len(row.keys())==3:
        print(row)
        row.clear() #reset

client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY)
 
client.on_connect    = connected
client.on_disconnect = disconnected
client.on_message    = message
client.connect()

client.loop_blocking()