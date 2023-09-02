# import module
import requests
import  json
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import os
from pyarrow.parquet import ParquetFile
from datetime import datetime
from  email_smtp import email_notify
import logging
import pickle
class Datapartition:
    def read_parquet_file():
        try: 
            path = "/opt/airflow/dags/parquet"
            if os.path.exists(path):
                print("inside if")
                for file in os.listdir(path):
                    df = ParquetFile(path+"/"+str(file))
                print(df) 
            parquet_file = pq.ParquetFile('/opt/airflow/dags/parquet/parquet_data.parquet')
            counter=0
            print(parquet_file)
            for val in parquet_file.iter_batches(batch_size=250):
                # print("batch")
                print(val.to_pandas())
                if val is not None:
                    df=pa.Table.from_batches([val]).to_pandas()
                    print(df)
                    # pq.write_table(table, f'/home/vazid/Downloads/ETL_Pipeline/partition_data/batch{counter}.parquet')
                    # print(val)
                    df.to_parquet(f'/opt/airflow/dags/partition_data/batch{counter}.parquet')
                    counter+=1
                    print(df)

                email_notify.email_send_requests('this is  partion data successfull')
                print(df)
                logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

                logging.info('partitioning_data  done ')
        except Exception as exception:
               logging.exception("Exception occured",exception)
               logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

               logging.error('partitioning_data not done')
# x=partition()
# # data_collector()
# x.read_parquet_file()