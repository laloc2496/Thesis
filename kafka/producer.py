from kafka import KafkaProducer
from object import Object
from utils import *
from Adafruit_IO import MQTTClient
import sys

import json


def to_json(object: Object):
    return json.dumps(object.__dict__)


def send_message(message, topic=TOPIC_KAFKA):
    producer.send(TOPIC_KAFKA, to_json(message).encode('utf-8'))


def connected(client, group_name):
    print('Listening for changes on ', group_name)
    client.subscribe_group(group_name)


def subscribe(client, userdata, mid, granted_qos):
    # This method is called when the client subscribes to a new feed.
    print('Subscribed to {0} with QoS {1}'.format(FEED_ID, granted_qos[0]))


def disconnected(client):
    # Disconnected function will be called when the client disconnects.
    print('Disconnected from Adafruit IO!')
    sys.exit(1)


def message(client, topic_id, payload, group):
    if LIGHT in payload.keys():
        DICT_GROUP_DATA[group].light = round(float(payload[LIGHT]), 1)
    if HUMIDITY in payload.keys():
        DICT_GROUP_DATA[group].humidity = float(payload[HUMIDITY])
    if SOIL in payload.keys():
        DICT_GROUP_DATA[group].soil = round(float(payload[SOIL]),1)
    if TEMPERATURE in payload.keys():
        DICT_GROUP_DATA[group].temperature = float(payload[TEMPERATURE])
    if DICT_GROUP_DATA[group].check():
        send_message(DICT_GROUP_DATA[group])
        DICT_GROUP_DATA[group].reset()


def connection_to_feed(group_name) -> MQTTClient:
    client = MQTTClient(ADAFRUIT_IO_USERNAME,
                        ADAFRUIT_IO_KEY, group=group_name)
    client.on_connect = connected
    client.on_disconnect = disconnected
    client.on_message = message
    client.connect()
    return client



if __name__ == "__main__":
    DICT_GROUP_DATA = dict()
    for name in GROUP_NAMES:
        DICT_GROUP_DATA[name] = Object(id=name)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    connection_to_feed(GROUP_NAMES[0]).loop_blocking()
    producer.close()
