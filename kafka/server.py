
import sys
import random,time
from utils import *
from Adafruit_IO import MQTTClient

group_name=None
def connected(client):
    print('Listening for changes on ', group_name)
    client.subscribe_group(group_name)

def subscribe(client, userdata, mid, granted_qos):
    # This method is called when the client subscribes to a new feed.
    print('Subscribed to {0} with QoS {1}'.format(FEED_ID, granted_qos[0]))

def disconnected(client):
    # Disconnected function will be called when the client disconnects.
    print('Disconnected from Adafruit IO!')
    sys.exit(1)

def message(client, topic_id, payload):
    print('Topic {0} received new value: {1}'.format(topic_id, payload))


# client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY)
 
# client.on_connect    = connected
# client.on_disconnect = disconnected
# client.on_message    = message
# client.connect()
# #client.loop_blocking()
# client.loop_background()
# while(True):
#     value = random.randint(0, 100)
#     for feed in feeds:
#         client.publish(str(feed),value,group_name)
#     time.sleep(10)