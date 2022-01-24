
from producer import connection_to_feed
import argparse
from datetime import datetime as dt
times_irrigation=[60,120,180,240]
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='This script used to send request irrigation')
    parser.add_argument("--id", type=str)
    parser.add_argument("--value", type=int)
    args = parser.parse_args()
    feed_id = args.id
    value = int(args.value)
    print('-'*10)
    print(f'{feed_id}:{value}')
    print(dt.now())
    try:
        client = connection_to_feed(feed_id)
        client.publish('motor', times_irrigation[value], feed_id)
        print("Success !")
    except:
        print("Can not send request")
    print('-'*10)