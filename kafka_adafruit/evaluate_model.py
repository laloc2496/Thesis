from Adafruit_IO import MQTTClient
from datetime import datetime as dt
from utils import get_account
from csv import writer

FEED_ID = "soil"
LIGHT = 'light'
MOTOR = 'motor'
SOIL = 'soil'
TEMPERATURE = 'temperature'
HUMIDITY = 'humidity'
feeds = [LIGHT, MOTOR, SOIL, TEMPERATURE, HUMIDITY]
TIMELINE = [("6:00", "9:59", 35), ("10:00", "16:59", 50),
            ("17:00", "5:59", 65)]

GROUP_NAMES = ['sensors', 'svm', 'bayes','dt']


def get_threshhold():
    for a, b, thresh_hold in TIMELINE[:-1]:
        a = dt.strptime(a, "%H:%M")
        b = dt.strptime(b, "%H:%M")
        now = dt.now().strftime("%H:%M")
        now = dt.strptime(now, "%H:%M")
        if now > a and now < b:
            return thresh_hold
    return TIMELINE[-1][2]

# Penalty:
# ko lệch: 0
# lệch <10: -1
# Lệch 10-15: -2
# Lệch >15: -3


def get_penalty(soil):
    threshhold = get_threshhold()
    print(threshhold)
    if soil < threshhold:
        value = threshhold-soil
        if value < 10:
            return -1
        if value >= 10 and value < 15:
            return -2
        if value >= 15:
            return -3
    return 0


def connected(client, group_name):
    print('Listening for changes on ', group_name)
    client.subscribe_group(group_name)


def disconnected(client):
    pass


def message(client, topic_id, payload, group):
    now = dt.now().strftime("%Y-%m-%d")
    csv = f'{group}.csv'

    print(f'Topic: {topic_id}   Group: {group} Payload: {payload}')
    if SOIL in payload.keys():
        soil = round(float(payload[SOIL]), 1)
        penalty = get_penalty(soil)
        file = f'evaluate_model/{now}/penalty/{group}.csv'
        with open(file, 'a') as fd:
            writer_object = writer(fd)
            writer_object.writerow([now, penalty, soil, get_threshhold()])
    if MOTOR in payload.keys():
        file = f'evaluate_model/{now}/motor/{group}.csv'
        with open(file, 'a') as fd:
            writer_object = writer(fd)
            writer_object.writerow([now, round(float(payload[MOTOR]), 1)])


def connection_to_feed(group_name) -> MQTTClient:
    account = get_account(group_name)
    client = MQTTClient(account.username,
                        account.key, group=group_name)
    client.on_connect = connected
    client.on_disconnect = disconnected
    client.on_message = message
    client.connect()
    return client


if __name__ == "__main__":

    DICT_GROUP_DATA = dict()
    for name in GROUP_NAMES:
        connection_to_feed(name).loop_background()
    while(True):
        pass
