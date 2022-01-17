#!/bin/bash

if pgrep -f "kafka_adafruit/producer.py" ; then
    echo "Producer alrealy running.."
else 
    nohup python3 kafka_adafruit/producer.py > producer_log.log &
    echo "Producer local is running"
fi  

if pgrep -f "kafka_adafruit/consumer.py" ; then
    echo "Consumer alrealy running.."
else 
    nohup spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka_adafruit/consumer.py > consumer_log.log &
    echo "Start consumer"
fi  

if pgrep -f "ensemble_model/main.py"; then
    echo "Main alrealy running.."
else 
    nohup python3 ensemble_model/main.py > main_log.log &
    echo "Start main"
fi  