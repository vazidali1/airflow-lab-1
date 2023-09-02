from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
from faker import Faker
import json

default_args = {
    "owner": "airflow",
    "retries": 1,
}

def fake_data_generator():
    fake = Faker()
    records = ""
    for i in range(1000):
        json_data = {
            "zip": fake.zipcode(),
            "birthday": fake.date_of_birth().__str__(),
            "country": fake.country(),
            "city": fake.city(),
            "sex": "M",
            "reasonfortermination": fake.paragraph(),
            "dateofhire": fake.date_object().__str__(),
            "performancescore": str(fake.random_digit()),
            "number": str(fake.random_digit()),
            "employmentstatus": fake.text(),
            "maritalstatus": "single",
            "dateoftermination": fake.date_object().__str__(),
            "name": fake.name(),
            "department": fake.job(),
            }
        records += json.dumps(json_data) + "\n"

    with open('/opt/airflow/files/fake_data.json', 'a') as outfile:
        outfile.write(records)


with DAG("first_data_pipeline", start_date=datetime.now(), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    first_operator = BashOperator(
        task_id='first_operator',
        bash_command="echo 'First operator executes.'"
    )

    data_generator = PythonOperator(
        task_id='data_generator',
        python_callable=fake_data_generator
    )

    fake_data_generate = BashOperator(
        task_id='fake_data_generate',
        bash_command="echo 'Fake data generate and save in docker container.'"
    )

    first_operator >> data_generator >> fake_data_generate
