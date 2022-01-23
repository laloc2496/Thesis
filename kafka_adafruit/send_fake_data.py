from utils import *
from Adafruit_IO import MQTTClient


def connected(client, group_name):
    print('Listening for changes on ', group_name)
    client.subscribe_group(group_name)


def subscribe(client, userdata, mid, granted_qos):
    # This method is called when the client subscribes to a new feed.
    print('Subscribed to {0} with QoS {1}'.format(FEED_ID, granted_qos[0]))


def disconnected(client):
    # Disconnected function will be called when the client disconnects.
    print('Disconnected from Adafruit IO!')


def message(client, topic_id, payload, group):
    print('Topic {0} received new value: {1}'.format(topic_id, payload))


group_name = 'sensors'
account = get_account(group_name)
# client = MQTTClient(ADAFRUIT_IO_USERNAME,
#                     ADAFRUIT_IO_KEY, group=group_name)
client = MQTTClient(account.username,
                    account.key, group=group_name)
client.on_connect = connected
client.on_disconnect = disconnected
client.on_message = message
client.connect()
client.loop_background()
data = [('humidity', 80.23), ('light', 20.23),
        ('soil', 50.55), ('temperature', 28.55)]


# client.loop_background()
while(True):
    if (input("Send? ") == "1"):
        for feed, value in data:
            client.publish(feed, value, group_name)
        # time.sleep(10)
