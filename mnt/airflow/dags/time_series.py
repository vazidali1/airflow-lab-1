# import module
import pandas as pd 
import dateutil
import os 
import pyarrow as pa 
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetFile
import pyarrow.dataset as ds
from email_smtp import email_notify
import logging
date_time=pd.read_parquet ('/opt/airflow/dags/parquet/parquet_data.parquet')

class Timeseries:
    def add_duration(path=""):
        try:

            path = "/opt/airflow/dags/partition_data"
            if (os.path.exists(path)):
                counter = 0
                for file in os.listdir(path):
                    print(path+"/"+str(file))
                    # df = ParquetFile(path+"/"+str(file))
                    df_timeseries = pd.read_parquet(path+"/"+str(file))
                    # Converting to date-type (if it's already in that format)
                    df_timeseries['tpep_pickup_datetime'] = df_timeseries['tpep_pickup_datetime'].apply(dateutil.parser.parse)

                    #Then use `tz_localize` to make timestamps aware about time zone and then convert to IST (Asia/Kolkata)
                    df_timeseries['Time Series_ist'] = pd.to_datetime(df_timeseries['tpep_pickup_datetime']).\
                                                                dt.tz_localize('utc').\
                                                                dt.tz_convert('Asia/Kolkata') 

                    df_timeseries['tpep_dropoff_datetime'] = df_timeseries['tpep_dropoff_datetime'].apply(dateutil.parser.parse)

                    df_timeseries['Time Series_ist'] = pd.to_datetime(df_timeseries['tpep_dropoff_datetime']).\
                                                                dt.tz_localize('utc').\
                                                                dt.tz_convert('Asia/Kolkata') 
                                                                
                    df_timeseries["Duration"] = (df_timeseries['tpep_dropoff_datetime'] - df_timeseries['tpep_pickup_datetime']).dt.total_seconds() / 60 

                    #converting float to int 
                    df_timeseries['Duration'] = df_timeseries['Duration'].astype('int')

                    df_timeseries.drop(['Unnamed: 0','tpep_pickup_datetime','tpep_dropoff_datetime','Time Series_ist'],axis = 1)

                    df_timeseries.to_csv(f'/opt/airflow/dags/duration/timeseries{counter}.csv')
                    
                    counter += 1
                logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")
                logging.info('time_series  done ')
        except :
                logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")
                logging.error('time series  not done ')     
  
email_notify.email_send_requests('this is successfull read_partion data')
      
# fun = Timeseries()
# fun.add_duration() 








