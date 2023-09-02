import requests 
import json
import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
from    airflow.operators.python_operator    import PythonOperator

# print("check for pyarrow: {}".format(dir(pq)))
from email_smtp  import email_notify
import logging 

import os
def Data_collector():
        try:
            #Connecting to API
            api_data = requests.get('https://data.cityofnewyork.us/resource/t29m-gskq.json')
            data = api_data.text
            #print(data)
            
            data1 = json.loads(data)
            
        #     #Converting data into Dataframe
            df = pd.DataFrame(data1)
            print(df.head(5))

        #     #Saving data as CSV file
            os.makedirs('/opt/airflow/dags/csv',  exist_ok=True  )  
            
            df.to_csv('/opt/airflow/dags/csv/all.csv' ) 

            #Reading CSV file
            df_new = pd.read_csv("/opt/airflow/dags/csv/all.csv")

            # print(df_new) 
    #     #converting csv file to parquet

            table = pa.Table.from_pandas(df_new)

            pq.write_table(table, '/opt/airflow/dags/parquet/parquet_data.parquet')
            print(df.head(5))



            email_notify.email_send_requests('data collected and save in parquet file successfully')
            
            logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

            logging.info('data collected and save in parquet file successfully')
            
        except Exception as exception:
            logging.exception("Exception occured",exception)
            logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

            logging.error('data not collected successfully')