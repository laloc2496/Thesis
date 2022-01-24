if pgrep -f "kafka_adafruit/soil.py"; then
    echo "svm alrealy running.."
else 
    nohup python3 kafka_adafruit/soil.py > logs/soil_log.out &
    echo "Start soil"
fi


if pgrep -f "kafka_adafruit/soil_bayes.py"; then
    echo "svm alrealy running.."
else 
    nohup python3 kafka_adafruit/soil_bayes.py > logs/soil_bayes_log.out &
    echo "Start soil bayes"
fi


if pgrep -f "kafka_adafruit/soil_dt.py"; then
    echo "svm alrealy running.."
else 
    nohup python3 kafka_adafruit/soil_dt.py > logs/soil_dt_log.out &
    echo "Start soil dt"
fi


if pgrep -f "kafka_adafruit/soil_svm.py"; then
    echo "svm alrealy running.."
else 
    nohup python3 kafka_adafruit/soil_svm.py > logs/soil_svm_log.out &
    echo "Start soil svm"
fi