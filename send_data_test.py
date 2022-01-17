import requests
import json
import io
# from avro.schema import Parse
# from avro.datafile import DataFileReader, DataFileWriter
# from avro.io import DatumReader, DatumWriter
url = "http://hpcc.hcmut.edu.vn:10027/data/push"
headers = {
  'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJ1c2VyX2FhNjVlMTVmLThkMWEtNGJiZC04Zjc3LTk1NGMzM2NiNTZlOCIsImlhdCI6MTY0MTgzMjc5MX0.nKVLaGS6X6n-zLCebeCavQeNIAp05TFtdB5ak-cINps',
  'Content-Type': 'application/json'
}
payload = json.dumps({
  "schema":"spark-smart-village",
  "data": {
    "id": "test",
    "humidity": 50.0,
    "soil": 65.0,
    "light":1200,
    "temperature": 31.0,
    "time": "04:46:13 15-01-2022"
  }
})

response = requests.request("POST", url, headers=headers, data=payload)
print(response.text)