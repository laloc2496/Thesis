
from producer import connection_to_feed
import argparse
import time
from datetime import datetime as dt
times_irrigation=[40,100,180,240]
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
        result=client.publish('motor', times_irrigation[value]*2, feed_id)
        if result.is_published() == False:
            print("Retry !")
            while result==False:
                result=client.publish('motor', times_irrigation[value]*2, feed_id)
                time.sleep(1)
        print("Success !")
    except Exception as e:
        print(e)
    print('-'*10)