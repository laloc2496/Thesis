from object import *
from kafka import KafkaProducer
import json
from utils import *
def to_json(object: Object):
    return json.dumps(object.__dict__)
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
def send_message(message, topic=TOPIC_KAFKA):
    print(to_json(message))
    producer.send(TOPIC_KAFKA, to_json(message).encode('utf-8'))


while True:
    if input()=="1":
        send_message(Object(id="sensors",humidity=20,light=100,soil=20,temperature=30))
        print("Sent message !")