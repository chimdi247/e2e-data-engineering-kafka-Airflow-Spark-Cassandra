import uuid
from datetime import datetime

# Airflow 3: DAG now lives in airflow.sdk, and PythonOperator moved to the standard provider
# (the old airflow.operators.python import still works but is deprecated).
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2026, 9, 3, 10, 00)
}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    # str(): a raw UUID object can't be serialised by json.dumps, so in the original every
    # message failed in stream_data() and nothing ever reached Kafka.
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    # The API returns a number for some countries and a string for others.
    data['post_code'] = str(location['postcode'])
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

    # send() is asynchronous; make sure everything is delivered before the task ends.
    producer.flush()

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',  # Airflow 3 removed schedule_interval
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
