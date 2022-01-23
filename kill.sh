kill $(ps aux | grep 'producer.py' | awk '{print $2}')
echo "killed producer"

kill $(ps aux | grep 'consumer.py' | awk '{print $2}')
echo "killed consumer"

kill $(ps aux | grep 'main.py' | awk '{print $2}')
echo "killed main"

kill $(ps aux | grep 'main_svm.py' | awk '{print $2}')
echo "killed main_svm"

kill $(ps aux | grep 'main_dt.py' | awk '{print $2}')
echo "killed main_dt"

kill $(ps aux | grep 'main_bayes.py' | awk '{print $2}')
echo "killed main_bayes"

kill $(ps aux | grep 'evaluate_model.py.py' | awk '{print $2}')
echo "killed evaluate_model.py"