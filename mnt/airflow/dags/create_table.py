# #import module
# import psycopg2
# import pyarrow.parquet as pq
# import requests 
# import json
# import pandas as pd 
# import pyarrow as pa
# import pyarrow.parquet as pq
# import os 
# from  email_smtp import email_notify
# import logging
# conn = psycopg2.connect(


# host='host.docker.internal', database='airflow_db',user='airflow', password='airflow', port=5432)
# cur = conn.cursor()

# # # create table report_data
# # df='''create table reportdata(
# #     id serial primary key,
# #     source varchar(255),
# #     comment varchar(255),
# #     answer boolean);
# #     '''
# # cur.execute(df)
# # conn.commit()

# # # create table locationreport
# # df1='''create  table locationreport(
# #     field_id serial primary key,
# #     field_name varchar(255),
# #     location text);'''
# # cur.execute(df1)
# # conn.commit()

# # # create table durationreport
# # df2=''' create table durationreport(
# #     file_id serial primary key,
# #      file_name varchar(255),
# #     duration text);'''
# # cur.execute(df2)
# # conn.commit()
# # # conn.close()
# # conn.close()











# # insert data:

# class Insert_data:

    
#     # data insert
#     def storing_db_location(self):
#         try:  
            
#             path = '/opt/airflow/dags/location'
#             insertion = """ 
#                     INSERT INTO locationreport(field_name,location) VALUES (%s,%s) """
#             print(path)
#             for file in os.listdir(path):
#                 file=open('/opt/airflow/dags/location' ,'a')
#                 print(file)
#                 df = pd.read_csv(os.path.join(path,file))
#                 print(df)
#                 df1 = list(df['location'])
#                 df1 = ",".join(df1) # converting the coulmn into string 
            
#                 cur.execute(insertion,(file,df1))
                    
#                 conn.commit()

#             # email_notify.email_send_requests('this is successfull storing_location')           
#             logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

#             logging.info('connect  done ')
#         except:
#             logging.basicConfig(filename='/opt/airflow/dags/logs/logss.log',format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

#             logging.error('connect not done ')   
                

# # data insert
#     def storing_db_duration(self):

#         try:
#             path = '/opt/airflow/dags/duration'
#             insertion = """ 
#                     INSERT INTO durationreport(file_name,duration) VALUES (%s,%s)
#                     """
#             path = open(" ","w")  
#             for file in os.listdir(path):
                
#                 # print(path)
#                 df = pd.read_csv(os.path.join(path,file))
                
#                 df1 = list(df['Duration'].astype(str))
#                 df1 = ",".join(df1) # converting the coulmn into string 
            
#                 cur.execute(insertion,(file,df1))
                    
#                 # conn.commit()
#             email_notify.email_send_requests('this is successfull storing duration data')

#             logging.basicConfig(filename='/opt/airflow/dags/logs/logss.log',format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

#             logging.info('connect data  done ')
#         except:
#             logging.basicConfig(filename='/opt/airflow/dags/logs/logss.log',format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

#             logging.error('connect data not done ')
            

            
# # # function call 
# self= Insert_data()
# # self.storing_db_location()
# # self.storing_db_duration()                    

# # # conn.close()  

    










