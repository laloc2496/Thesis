kill $(ps aux | grep 'producer.py' | awk '{print $2}')
echo "killed producer"

kill $(ps aux | grep 'consumer.py' | awk '{print $2}')
echo "killed consumer"

kill $(ps aux | grep 'main.py' | awk '{print $2}')
echo "killed main"

kill $(ps aux | grep 'evaluate_model.py' | awk '{print $2}')
echo "killed evaluate_model.py"


#!/bin/bash
echo "try to kill old processes"

bash kill.sh

echo "create new processes"

if pgrep -f "kafka_adafruit/producer.py" ; then
    echo "Producer alrealy running.."
else 
    nohup python3 kafka_adafruit/producer.py > logs/producer_log.out &
    echo "Start producer"
fi  


#consumer------------------
if pgrep -f "kafka_adafruit/consumer.py" ; then
    echo "Consumer alrealy running.."
else 
    nohup spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka_adafruit/consumer.py > logs/consumer_log.out &
    echo "Start consumer"
fi  


#main---------------

if pgrep -f "ensemble_model/main.py"; then
    echo "Main alrealy running.."
else 
    nohup python3 ensemble_model/main.py > logs/log_main.out &
    echo "Start main sensors"
fi  

#evaluate--------------------
if pgrep -f "kafka_adafruit/evaluate_model.py"; then
    echo "evaluate alrealy running.."
else 
    nohup python3 kafka_adafruit/evaluate_model.py > logs/evaluate_log.out &
    echo "Start evaluate"
fi  