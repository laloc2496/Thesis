from kafka import KafkaProducer
from object import Object
from utils import *
from Adafruit_IO import MQTTClient
import sys

def connected(client,group_name):
    print('Listening for changes on ', group_name)
    client.subscribe_group(group_name)

def subscribe(client, userdata, mid, granted_qos):
    # This method is called when the client subscribes to a new feed.
    print('Subscribed to {0} with QoS {1}'.format(FEED_ID, granted_qos[0]))

def disconnected(client):
    # Disconnected function will be called when the client disconnects.
    print('Disconnected from Adafruit IO!')
    sys.exit(1)

def message(client, topic_id, payload,group):
    if LIGHT in payload.keys():
        DICT_GROUP_DATA[group].light= round(float(payload[LIGHT]),1)
    if HUMIDITY in payload.keys():
        DICT_GROUP_DATA[group].humidity= float(payload[HUMIDITY])
    if SOIL in payload.keys():
        DICT_GROUP_DATA[group].soil= float(payload[SOIL])
    if TEMPERATURE in payload.keys():
        DICT_GROUP_DATA[group].temperature= float(payload[TEMPERATURE])    
    # print(DICT_GROUP_DATA[group].light)
    # print(DICT_GROUP_DATA[group].humidity)
    # print(DICT_GROUP_DATA[group].temperature) 

    if DICT_GROUP_DATA[group].check():
        DICT_GROUP_DATA[group].reset()

def connection_to_feed(group_name) -> MQTTClient : 
    client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY,group=group_name)
    client.on_connect    = connected
    client.on_disconnect = disconnected
    client.on_message    = message
    client.connect()
    return client

DICT_GROUP_DATA=dict()
for name in GROUP_NAMES:
    DICT_GROUP_DATA[name]= Object(id=name)

connection_to_feed(GROUP_NAMES[0]).loop_blocking()
# producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
# objects= list()
# for _ in range(100):
#     producer.send(TOPIC_KAFKA,b'14')

# producer.close()