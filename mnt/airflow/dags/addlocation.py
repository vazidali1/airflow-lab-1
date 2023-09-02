import pandas as pd 
import os 
import pyarrow as pa 
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetFile
import pyarrow.dataset as ds
import logging
from airflow.operators.python_operator import   PythonOperator

from email_smtp import email_notify

class  FetchLocation:       

    def get_location(value):
            if value == 142:
                return "Noida Sec-1"
            elif value == 237:
                return "Noida Sec-2"
            elif value == 161:
                return "Noida Sec-3"
            elif value == 79:
                return "Noida Sec-4"
            elif value == 170:
                return "Noida Sec-5"
            elif value == 48:
                return "Noida Sec-6"
            elif value == 263:
                return "Noida Sec-7"
            elif value == 132:
                return "Noida Sec-8"
            elif value == 249:
                return "Noida Sec-9"
            elif value == 236:
                return "Noida Sec-10"
            elif value == 138:
                return "Noida Sec-11"
            elif value == 164:
                return "Noida Sec-12"
            elif value == 163:
                return "Noida Sec-13"
            elif value == 148:
                return "Noida Sec-14"
            elif value == 238:
                return "Noida Sec-15"
            elif value == 113:
                return "Noida Sec-16"
            elif value == 50:
                return "Noida Sec-17"
            elif value == 229:
                return "Noida Sec-18"
            elif value == 162:
                return "Noida Sec-19"
            elif value == 231:
                return "Noida Sec-20"
            elif value == 43:
                return "Noida Sec-21"
            elif value == 68:
                return "Noida Sec-22"
            elif value == 186:
                return "Noida Sec-23"
            elif value == 158:
                return "Noida Sec-24"
            elif value == 141:
                return "Noida Sec-25"
            elif value == 144:
                return "Noida Sec-26"
            elif value == 239:
                return "Noida Sec-27"
            elif value == 75:
                return "Noida Sec-28"
            elif value == 125:
                return "Noida Sec-29"
            elif value == 264:
                return "Noida Sec-30"
            elif value == 140:
                return "Noida Sec-31"
            elif value == 151:
                return "Noida Sec-32"
            elif value == 246:
                return "Noida Sec-33"
            elif value == 234:
                return "Noida Sec-34"
            elif value == 211:
                return "Noida Sec-35"
            elif value == 166:
                return "Noida Sec-36"
            elif value == 233:
                return "Noida Sec-37"
            elif value == 137:
                return "Noida Sec-38"
            elif value == 244:
                return "Noida Sec-39"
            elif value == 116:
                return "Noida Sec-40"
            elif value == 262:
                return "Noida Sec-41"
            elif value == 90:
                return "Noida Sec-42"
            elif value == 42:
                return "Noida Sec-43"
            elif value == 129:
                return "Noida Sec-44"
            elif value == 62:
                return "Noida Sec-45"
            elif value == 41:
                return "Noida Sec-46"
            elif value == 69:
                return "Noida Sec-47"
            elif value == 261:
                return "Noida Sec-48"
            elif value == 114:
                return "Noida Sec-49"
            elif value == 107:
                return "Noida Sec-50"
            elif value == 229:
                return "Noida Sec-51"
            elif value == 143:
                return "Noida Sec-52"
            elif value == 13:
                return "Noida Sec-53"
            elif value == 255:
                return "Noida Sec-54"
            elif value == 243:
                return "Noida Sec-55"
            elif value == 74:
                return "Noida Sec-56"
            elif value == 189:
                return "Noida Sec-57"
            elif value == 65:
                return "Noida Sec-58"
            elif value == 168:
                return "Noida Sec-59"
            elif value == 87:
                return "Noida Sec-60"
            elif value == 45:
                return "Noida Sec-61"
            elif value == 88:
                return "Noida Sec-62"
            elif value == 100:
                return "Noida Sec-63"
            elif value == 209:
                return "Noida Sec-64"
            elif value == 152:
                return "Noida Sec-65"
            elif value == 24:
                return "Noida Sec-66"
            elif value == 181:
                return "Noida Sec-67"
            elif value==193:
                return "Noida sec-68"
            elif value==230:
                return "Noida sec-69"
            elif value==70:
                return "Noida sec-70"
            elif value==4:
                return "Noida sec-71"
            elif value==145:
                return "Noida sec-72"
            elif value==7:
                return "Noida sec-73"
            elif value==232:
                return "Noida sec-74"
            elif value==224:
                return " Noida sec-75"
            elif value==179:
                return "Noida sec-76"
            elif value==249:
                return "Noida sec_77"
            elif value==119:
                return "Noida sec-78"
            elif value==74:
                return "Noida sec_79"
            elif value==68:
                return "Noida sec-80"
            elif value==260:
                return "Noida sec-81"
            elif value==37:
                return " Noida sec-82"
            elif value==265:
                return "Noida sec=83"
            elif value==228:
                return "Noida sec-84"
            elif value==196:
                return "Noida sec_85"
            elif value==146:
                return "Noida sec-86"
            elif value==66:
                return "Noida sec=88"
            elif value==96:
                return "Noida sec-89"
            elif value==35:
                return "Noida sec-90"
            elif value==80:
                return "Noida sec-91"
            elif value==226:
                return "Noida sec-92"
            elif value==97:
                return "Noida sec_93"
        

    def read_partition_data():
        """
        read the partition data
        """
        try:
            path = "/opt/airflow/dags/partition_data"
            if (os.path.exists(path)):   
                counter = 0
                for file in os.listdir(path):
                    print("copmplete file path-->", path+"/"+str(file))
                    # dpdf = ParquetFile(path+"/"+str(file))
                    df_location = pd.read_parquet(path+"/"+str(file))
            
                    df_location['location'] = df_location['pulocationid'].map(FetchLocation.get_location)

                    df_location.drop(['Unnamed: 0'],axis = 1)

                    df_location.to_csv(f'/opt/airflow/dags/location/location{counter}.csv') 
                    counter += 1
            logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

            logging.info('add_location  done ')  
        except Exception as exception:

            logging.exception("Exception occured",exception)
            logging.basicConfig(filename="/opt/airflow/dags/parquet/parquet_data/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

            logging.error('add_location  not done ')  
            logging.exception('exception')
                  
# fun = add_location() 
# fun.read_partition_data()
email_notify.email_send_requests('this is successfull add location data')