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


#soil--------------------------------
bash soil.sh

#main---------------

if pgrep -f "ensemble_model/main.py"; then
    echo "Main alrealy running.."
else 
    nohup python3 ensemble_model/main.py > logs/sensors_log.out &
    echo "Start main sensors"
fi  


if pgrep -f "ensemble_model/main_svm.py"; then
    echo "svm alrealy running.."
else 
    nohup python3 ensemble_model/main_svm.py > logs/svm_log.out &
    echo "Start main svm"
fi  


if pgrep -f "ensemble_model/main_dt.py"; then
    echo "dt alrealy running.."
else 
    nohup python3 ensemble_model/main_dt.py > logs/dt_log.out &
    echo "Start main dt"
fi  


if pgrep -f "ensemble_model/main_bayes.py"; then
    echo "bayes alrealy running.."
else 
    nohup python3 ensemble_model/main_bayes.py > logs/bayes_log.out &
    echo "Start main bayes"
fi  
 
#evaluate--------------------
if pgrep -f "kafka_adafruit/evaluate_model.py"; then
    echo "evaluate alrealy running.."
else 
    nohup python3 kafka_adafruit/evaluate_model.py > logs/evaluate_log.out &
    echo "Start evaluate"
fi  