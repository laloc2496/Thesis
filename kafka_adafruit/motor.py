
from producer import connection_to_feed
import argparse
from datetime import datetime as dt
times_irrigation=[60,90,120,150]
if __name__ == "__main__":
    print('-'*10)
    print(dt.now())
    parser = argparse.ArgumentParser(
        description='This script used to send request irrigation')
    parser.add_argument("--id", type=str)
    parser.add_argument("--value", type=int)
    args = parser.parse_args()
    feed_id = args.id
    value = int(args.value)
    try:
        client = connection_to_feed('sensor')
        client.publish('motor', times_irrigation[value]*5, feed_id)
        print("Success !")
    except:
        print("can not send request")
    print('-'*10)