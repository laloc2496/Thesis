from server import *
client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY)
 
client.on_connect    = connected
client.on_disconnect = disconnected
client.on_message    = message
client.connect()
client.loop_background()
while(True):
    if (input("Send? ")=="1"):
        value = random.randint(0, 100)
        for feed in feeds[:-1]:
            client.publish(str(feed),value,group_name)
        #time.sleep(10)
    time.sleep(1)