 
## Usage


Run comsumer to store data into HDFS **10.1.8.7** (REQUIRED):
```
spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka_adafruit/consumer.py
```

Run this command to start flow prediction **10.1.8.7** (REQUIRED):
Must sure this script NOT run in ```ensemble_model``` folder
```
python3 ensemble_model/main.py
```

### Optional

Run Mlflow server in **10.1.8.7** (if server dead): (Run this script in ```/home/binh```)
```
mlflow server --backend-store-uri sqlite:///mlflow.sqlite --default-artifact-root $ARTIFACT_ROOT --host 10.1.8.7 --port 5000
```

Retrain model (**10.1.8.7**): 
```
spark-submit ensemble_model/EnsembleStacking.py -t /home/binh/Thesis/ensemble_model/data/sample_data_test.csv -f humidity light
```

Prediction with custom data  and send request irrigation to motor. Check data irrigation in [this link](https://io.adafruit.com/quangbinh/feeds/sensors.motor)
```
python3 ensemble_model/EnsembleStacking.py -p data/sensors/partition=13-28-December-2021 --id sensors -f humidity light
```

## Format data send to Kafka
```
{
    "id": str,                  
    "humidity": float,  
    "soil": float,         
    "light: float,              
    "temperature": float        
}
```