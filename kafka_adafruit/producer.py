import time
import requests
from kafka import KafkaProducer
from object import Object
from utils import *
from Adafruit_IO import MQTTClient
import sys
import argparse
import json


def to_json(object: Object):
    return json.dumps(object.__dict__)


def send_message(message: Object, topic=TOPIC_KAFKA):
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
        light = round(float(payload[LIGHT]), 1) if float(
            payload[LIGHT]) > 1 else 0.001
        
        if group=='sensors':
            DICT_GROUP_DATA[group].light = light
            DICT_GROUP_DATA['svm'].light = light
            DICT_GROUP_DATA['dt'].light = light
            DICT_GROUP_DATA['bayes'].light = light
    if HUMIDITY in payload.keys():
        humidity = round(float(payload[HUMIDITY]), 1)

        if group=='sensors':
            DICT_GROUP_DATA[group].humidity = humidity
            DICT_GROUP_DATA['svm'].humidity = humidity
            DICT_GROUP_DATA['dt'].humidity = humidity
            DICT_GROUP_DATA['bayes'].humidity = humidity
    if SOIL in payload.keys():
        DICT_GROUP_DATA[group].soil = round(float(payload[SOIL]), 1)
    if TEMPERATURE in payload.keys():
        temperature = round(float(payload[TEMPERATURE]), 1)
        if group=='sensors':
            DICT_GROUP_DATA[group].temperature = temperature
            DICT_GROUP_DATA['svm'].temperature = temperature
            DICT_GROUP_DATA['dt'].temperature = temperature
            DICT_GROUP_DATA['bayes'].temperature = temperature
    for key in DICT_GROUP_DATA.keys():
        if DICT_GROUP_DATA[key].check():
            print(f'Send message {key}')
            if HPC == False:      
                send_message(DICT_GROUP_DATA[key])
                DICT_GROUP_DATA[key].reset()
                time.sleep(15)
            else:
                send_message_to_HPC(DICT_GROUP_DATA[key])
                DICT_GROUP_DATA[key].reset()
                time.sleep(15)


def connection_to_feed(group_name) -> MQTTClient:
    account = get_account(group_name)
    # client = MQTTClient(ADAFRUIT_IO_USERNAME,
    #                     ADAFRUIT_IO_KEY, group=group_name)
    client = MQTTClient(account.username,
                        account.key, group=group_name)
    client.on_connect = connected
    client.on_disconnect = disconnected
    client.on_message = message
    client.connect()
    return client


def send_message_to_HPC(message):
    # IP 10.1.8 can not send request
    TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJ1c2VyX2FhNjVlMTVmLThkMWEtNGJiZC04Zjc3LTk1NGMzM2NiNTZlOCIsImlhdCI6MTY0MTgzMjc5MX0.nKVLaGS6X6n-zLCebeCavQeNIAp05TFtdB5ak-cINps"
    user_id = "user_aa65e15f-8d1a-4bbd-8f77-954c33cb56e8"
    NAME = "binh"
    message = to_json(message)
    print("Send message to HPC")
    url = "http://hpcc.hcmut.edu.vn:10027/data/push"
    payload = json.dumps({
        "data":   json.loads(message)})
    headers = {
        'Authorization': TOKEN,
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)


#topic: topic1
"""
format input:
{
    "id": str,                  REQUIRED
    "humidity": float,          REQUIRED
    "soil": float,              REQUIRED
    "light: float,              REQUIRED
    "temperature": float        REQUIRED
}

"""
#HPC = False
if __name__ == "__main__":

    GROUP_NAMES = ['sensors', 'svm', 'dt', 'bayes']
    parser = argparse.ArgumentParser(
        description='This script used to send data to Kafka')
    parser.add_argument("--hpc", '-p', action="store_true", default=False)
    args = parser.parse_args()
    global HPC
    HPC = args.hpc
    if HPC:
        print('Send data to HPC')
    else:
        print('Send data to local kafka')
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    DICT_GROUP_DATA = dict()
    for name in GROUP_NAMES:
        DICT_GROUP_DATA[name] = Object(id=name)
        connection_to_feed(name).loop_background()
    while True:
        pass
    producer.close()
