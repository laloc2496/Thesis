mlflow server --backend-store-uri $BACKEND_URI --default-artifact-root $ARTIFACT_ROOT --host 10.1.8.7 --port 5000

mlflow server --backend-store-uri sqlite:///mlflow.sqlite --default-artifact-root $ARTIFACT_ROOT --host 10.1.8.7 --port 5000