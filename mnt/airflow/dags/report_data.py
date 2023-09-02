#report
#for max location and min location
import pandas as pd 

import os 

import csv

from email_smtp import email_notify
import logging


# class for locations 

class report: 

    def maxlocation(self,path):
    
        print('inside maxlocation')
        df = pd.read_csv(path)
        df1 = df.location.value_counts()
        print('df1')
    


        return(df1.head(10))
    
    def minlocation(self,path):
        print('inside minlocation')
        df = pd.read_csv(path)
        print('df')

        df11 = df.location.value_counts()
        print('df11')
 
    
    
        return(df11.tail(10))
        

# function to read csv
      
    def read_location_data(self):
        logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a") 
        logging.info("inside read_location")
        path = "/opt/airflow/dags/location"

        
        try:
        
            if (os.path.exists(path)):
                logging.info("inside if condition")
                counter = 0
                for file in os.listdir(path):
              
            
                    logging.info("inside for loop: {}".format(file))
                    # print(path+"/"+str(file))
                    max = self.maxlocation(path+"/"+str(file))
                    logging.info('max')
                    max.to_csv(f'/opt/airflow/dags/report/locationreport/locationreportmax{counter}.csv')
                    min = self.minlocation(path+"/"+str(file))
                    logging.info('max')
                    min.to_csv(f'/opt/airflow/dags/report/locationreport/locationreportmin{counter}.csv')
                    logging.info('min')
              
                    print('Exception error condition1')
                    counter = counter+1

            
            email_notify.email_send_requests('this is read data successfull')

            
            logging.info('report_data done ')
            
            
        except Exception as e:
            logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

            logging.error('report_data not done {}'.format(str(e)))
    #class for duration

    def max_duration(self,path):
        
            print("inside max_duration")
            df = pd.read_csv(path)
            df111 = df.Duration.value_counts()
            print('df111')
            return(df111.head(10))
   
            

    def min_duration(self,path):
        
            print("inside min_duration")

            df = pd.read_csv(path)
            df1111 = df.Duration.value_counts()
            print('df1111')
        
            return(df1111.tail(10)) 
           
    


    def read_timeseries_data(self):
        logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a") 
        logging.info("inside read_location")
        path = "/opt/airflow/dags/duration"



        try:
            if (os.path.exists(path)):
                logging.info("inside if condition second")
                counter = 0

                
                for file in os.listdir(path):
                    print('file')
                    logging.info("inside for loop second: {}".format(file))
                    print(path+"/"+str(file))
                    max = self.max_duration(path+"/"+str(file))
                    print('max')
                    logging.info('max')
                    max.to_csv(f'/opt/airflow/dags/report/durationreport/durationreportmax{counter}.csv')
                    
                    min = self.min_duration(path+"/"+str(file))
                    logging.info('min')

                    min.to_csv(f'/opt/airflow/dags/report/durationreport/durationreportmin{counter}.csv')
                    print('min')
                    counter = counter+1
                    logging.info('report_data done ')
            
            
        except Exception as e:
            logging.basicConfig(filename="/opt/airflow/dags/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

            logging.error('report_data not done {}'.format(str(e)))
        # except Exception as e:
        #             logging.error('report_data not done {}'.format(str(e)))
        #     # email_notify.email_send_requests('this is successfull')
        #     logging.basicConfig(filename='/opt/airflow/dags/logs/logss.log',format='%(asctime)s - %(message)s', level=logging.info,filemode="a")

        #     logging.info('reporty data done ')

        # except Exception as exception:
      
        #     logging.basicConfig(filename='/opt/airflow/dags/logs/logss.log',format='%(asctime)s - %(message)s', level=logging.info,filemode='a')
        #     logging.exception("Exception occured",exception)
        #     logging.error('report data not done ')
        #     logging.exception('exception ')

                
xyz = report()
xyz.read_location_data()
xyz.read_timeseries_data()
