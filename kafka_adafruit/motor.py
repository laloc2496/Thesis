
from producer import connection_to_feed
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='This script used to send request irrigation')
    parser.add_argument("--id", type=str)
    parser.add_argument("--value", type=int)
    args = parser.parse_args()
    feed_id = args.id
    value = int(args.value)
    client = connection_to_feed('sensor')
    client.publish('motor', value*100, feed_id)
