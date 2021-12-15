
from typing import Dict, Tuple
import requests
import random
import time
from threading import Thread
from server import *
import datetime


# soil = 50
# up = False
# irrigation_time = 0  # seconds
# update_time = 1
# alpha = 500
# beta = 0.1
SLEEP = 5


def get_message(client, topic_id, payload: Dict):
    now = datetime.datetime.now()
    if "motor" in payload.keys():
        irrigation_time = int(payload['motor'])
        if irrigation_time >= 0:
            soil.up = True
            soil.irrigation_time = irrigation_time
        print(irrigation_time)

    if "light" in payload.keys():
        soil.light = float(payload['light'])
    if "temperature" in payload.keys():
        soil.temperature = float(payload['temperature'])
    if "humidity" in payload.keys():
        soil.humidity = float(payload['humidity'])

    if now.hour >= 6 and now.hour <= 10:
        soil.rank = 1
    elif now.hour >= 19 and now.hour < 23:
        soil.rank = 0.7
    elif now.hour >=23 or now.hour <6:
        soil.rank =0.3
    elif now.hour >= 16 and now.hour < 19:
        soil.rank = 1.1
    else:
        soil.rank = 1.3

    if soil.up == False and soil.humidity>=70 and soil.temperature<=28:
        rain= random.randint(1,10)
        rain=  rain - ((soil.humidity-70)+ (30- soil.temperature)*2)//10 
        decision= (30- soil.temperature)*2+ soil.humidity-70
        if rain<3:
            soil.up=True
            soil.irrigation_time=10

class Soil:
    def __init__(self, value=50, irrigation_time=0, alpha=500, beta=0.1, sleep=SLEEP):
        self.light = 0
        self.temperature = 0
        self.humidity = 0
        self.value = value
        self.up = False
        self.irrigation_time = irrigation_time
        self.alpha = 500
        self.beta = 0.1
        self.sleep = 5
        self.rank = 1

    def update(self, value):
        self.value -= value
        time.sleep(self.sleep)

    def uptrend(self):
        print("uptrend")
        beta = self.beta
        irrigation_time = self.irrigation_time
        before = self.value
        rand1 = [random.uniform(-2*beta, 2*beta)
                 for x in range(int(irrigation_time/3))]
        rand2 = [random.uniform(-3*beta, 4*beta)
                 for x in range(int(2*irrigation_time/3))]
        rand3 = [random.uniform(-3*beta, 2.5*beta)
                 for x in range(int(0.1*irrigation_time))]
        rand4 = [random.uniform(-3*beta, 3*beta)
                 for x in range(int(0.5*irrigation_time))]
        for x in rand1+rand2+rand3+rand4:
            if self.value > 100:
                self.update(0)
            else:
                self.update(-x)
        self.up = False
        print("uptrend take ", len(rand1+rand2),
              "  from ", before, "  to ", self.value)
        self.downtrend()

    def downtrend(self):
        print("downtrend")
        beta = self.beta
        alpha = self.alpha
        rand1 = [random.uniform(-2*beta, 3*beta)
                 for x in range(0, int(random.uniform(alpha, 2*alpha)))]
        rand2 = [random.uniform(-3*beta, 5*beta)
                 for x in range(0, int(random.uniform(3*alpha, 4*alpha)))]
        #rand3 = [random.uniform(-0.1,0.1) for x in  range(0, inf)]
        for x in rand1+rand2:
            if self.up:
                self.uptrend()
            self.update(x*self.rank)
            if self.value < 20:
                break
        print("downtrend finish")
        while(self.value>=0):
            if self.up: self.uptrend()
            self.update(random.uniform(-0.5 * beta,beta))
            if (self.value <0 ): self.value= 0


def send(client, soil: Soil):

    while True:
        if soil.value < 0:
            soil.value = 0
        client.publish("soil", soil.value, group_name)
        time.sleep(SLEEP)


if __name__ == "__main__":
    client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY)
    client.on_connect = connected
    client.on_disconnect = disconnected
    client.on_message = get_message
    client.connect()
    client.loop_background()
    global soil
    soil = Soil()
    thread2 = Thread(target=soil.downtrend)
    thread2.start()
    send(client, soil)
