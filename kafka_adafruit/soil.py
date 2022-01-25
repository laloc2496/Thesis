from typing import Dict
import random 
import time 
from threading import Thread
from Adafruit_IO import MQTTClient
from utils import *
 
 
soil = 50
up = False
irrigation_time = 0 # seconds
update_time = 1
alpha = 500
beta = 0.1
SLEEP=180
def get_message(client, topic_id, payload,group):
    global irrigation_time,up
    if "motor" in payload.keys():
        
        irrigation_time= int(payload['motor'])
        if irrigation_time >=0:
            up=True
        print(irrigation_time)
 
  
def connected(client, group_name):
    print('Listening for changes on ', group_name)
    client.subscribe_group(group_name)

def disconnected(client):
    # Disconnected function will be called when the client disconnects.
    print('Disconnected from Adafruit IO!')
 



def change(value): 
  global soil , update_time
  soil = soil - value 
  print(soil)
  time.sleep(update_time)
def uptrend(): 
  print("uptrend")
  
  global soil,irrigation_time, up, beta
  before = soil
  #irrigation_time = int(random.uniform(0,1800)) # tưới trong bao lâu
  rand1 = [random.uniform(-2*beta,3*beta) for x in  range( int(irrigation_time/3) )]
  rand2 = [random.uniform(-3*beta,4*beta) for x in  range(int(2*irrigation_time/3) )]
  rand3 = [random.uniform(-3*beta,2.5*beta) for x in  range(int(0.1*irrigation_time) )]
  rand4 = [random.uniform(-3*beta,3*beta) for x in  range(int(0.5*irrigation_time) )]
  for x in rand1+rand2+rand3+rand4:
    if soil>100: change(0)
    else: 
      change(-x)

  up = False
  print("uptrend take ", len(rand1+rand2), "  from ", before, "  to ", soil)
  downtrend()
def downtrend():
  print("downtrend")
  global soil, alpha , beta
  rand1 = [random.uniform(-2*beta,3*beta) for x in  range(0, int(random.uniform(alpha,2*alpha)))]
  rand2 = [random.uniform(-2*beta,3*beta) for x in range(0, int(random.uniform(3*alpha,4*alpha)))]
  #rand3 = [random.uniform(-0.1,0.1) for x in  range(0, inf)]
  for x in rand1+rand2:
    if up: uptrend()    
    change(x)
    if soil<30: 
      break
    time.sleep(10)
  while(soil>=0):
    if up: uptrend()
    change(random.uniform(-1*beta,2*beta))
    time.sleep(10)
    if (soil <0 ): soil= 0

def send(client):
    global soil
    while True:
        if soil <0:
            soil=0
        client.publish("soil",soil,group_name)
        time.sleep(SLEEP)
        

#client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY, group=GROUP_NAMES[0])
group_name='sensors'
account=get_account(group_name)
client = MQTTClient(account.username,
                    account.key, group=group_name)
client.on_connect    = connected
client.on_disconnect = disconnected
client.on_message    = get_message
client.connect()
client.loop_background()
thread2 = Thread(target = downtrend)
thread2.start()
send(client)

