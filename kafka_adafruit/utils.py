from tokenize import group


BOOTSTRAP_SERVER = '10.1.8.8:9092'
# BOOTSTRAP_SERVER='10.1.1.24:9092'
TOPIC_KAFKA = 'topic1'
GROUP_NAMES = ['sensors']
ADAFRUIT_IO_USERNAME = "quangbinh"
ADAFRUIT_IO_KEY = "aio_grNV97RinHfoAC6LXFOOACyoU7PI"
SPARK_MASTER = "spark://10.1.8.7:7077"
FEED_ID = "soil"
LIGHT = 'light'
MOTOR = 'motor'
SOIL = 'soil'
TEMPERATURE = 'temperature'
HUMIDITY = 'humidity'
DATETIME_FORMAT = "%H:%M:%S %d-%m-%Y"
FEEDS = ['light', 'motor', 'soil', 'temperature', 'humidity']


class Account():
    def __init__(self, username, key, group_id) -> None:
        self.username = username
        self.key = key
        self.group_id = group_id


def get_account(name) -> Account:

    ACCOUNT = dict()
    ACCOUNT['sensors'] = Account(
        username="quangbinh",
        key="aio_grNV97RinHfoAC6LXFOOACyoU7PI",
        group_id='sensors'
    )
    ACCOUNT['bayes'] = Account(
        username="quangbinh",
        key="aio_grNV97RinHfoAC6LXFOOACyoU7PI",
        group_id='bayes'
    )
    ACCOUNT['svm'] = Account(
        username="quangbinh1",
        key="aio_tXfa371mjvnrD0yc6LTWyxNud0oZ",
        group_id='svm'
    )
    ACCOUNT['dt'] = Account(
        username="quangbinh1",
        key="aio_tXfa371mjvnrD0yc6LTWyxNud0oZ",
        group_id='dt'
    )
    return ACCOUNT[name]
