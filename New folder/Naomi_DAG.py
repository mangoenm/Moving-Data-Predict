#import airflow
import csv
import boto3
import psycopg2
from airflow import DAG
#from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash import BashOperator
from airflow.operators.bash_python import BashOperator
from datetime import datetime
#from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
#'airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook
#from airflow.providers.some_provider.hooks import SomeHook

 
HOME_DIR = "/home/ubuntu/"
#"/home/ubuntu/"
#"/opt/airflow"

 
# Insert your mount folder
#/home/ubuntu/s3-drive
# ==============================================================
 
# The default arguments for your Airflow, these have no reason to change for the purposes of this predict.
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': False,
   'email_on_retry': False,
}
 
# The function that uploads data to the RDS database, it is called upon later.
 
def upload_to_postgres():
   conn = psycopg2.connect(
   dbname = 'postgres',
   user = 'mangoenm',
   password = 'mangoenm',
   host = 'de-mbd-predict-naomi-mangoejane-rds-instance.cyg5kxo7cs9q.eu-west-1.rds.amazonaws.com',
   port = '5432')
 
   # Write a function that will upload data to your Postgres Database
   # Extract kwargs
   with open (f"{HOME_DIR}/moving_big_data/Scripts/insert_query.sql", "r") as f:
       query = f.read()
       database_connection_string = conn
   
   # Connect to the PostgreSQL database
   cursor = conn.cursor()
   
   try:
       # Upload data to PostgreSQL
       # Example code for uploading data to PostgreSQL goes here
       
       # Get data from the specified folder
       with open (f"{HOME_DIR}/moving_big_data/Output/historical_stock_data.csv", "r") as stockdata:
           read = csv.reader(stockdata)
           next(read)
           for stock, pair in enumerate (read):
               print(stock)
               cursor.execute(query, pair)
               if stock > 15000:
                   break
   finally:
       # Close the cursor and connection
       conn.commit()
       cursor.close()
       conn.close()
 
   # Example code for uploading data to Postgres goes here
   #return "Data uploaded to PostgreSQL and fetched from the folder"
   return "CSV Uploaded to postgres database"
 
def failure_sns(context):
   # Write a function that will send a failure SNS notification
   # Example code for sending failure SNS notification goes here
   sns_hook = AwsBaseHook('aws_default')
   sns_client = boto3.client('sns', region_name = 'eu-west-1')
   response = sns_client.publish(
           TopicArn='arn:aws:sns:eu-west-1:445492270995:2301ft-mbd-predict-naomi-mangoejane-SNS',
           Subject= "2304PTDE_Naomi_Mangoejane_Pipeline_Failure",
           Message= "2304PTDE_Naomi_Mangoejane_Pipeline_Failure"
       )
   print("SNS notification sent successfully:", response)
   return "Failure SNS Sent"
 
def success_sns(context):
   # Write a function that will send a success SNS Notification
   # Example code for sending success SNS notification goes here
   sns_hook = AwsBaseHook('aws_default')
   sns_client = boto3.client('sns', region_name = 'eu-west-1')
   response = sns_client.publish(
           TopicArn='arn:aws:sns:eu-west-1:445492270995:2301ft-mbd-predict-naomi-mangoejane-SNS',
           Subject= "2304PTDE_Naomi_Mangoejane_Pipeline_Success",
           Message= "2304PTDE_Naomi_Mangoejane_Pipeline_Success"
       )
   print("SNS notification sent successfully:", response)
   return "Success SNS sent"
 
# The dag configuration ===========================================================================
 
# Define your DAG configuration
dag = DAG('NAOMAN_DAG',  
   default_args=default_args,
   description='Naomi Mangoejane DAG',
   schedule=None,  # Define your schedule interval if needed
   start_date=datetime(2024, 3, 9),  # Define your start date
   catchup=False,  # Set to False if you don't want historical runs
   tags=['data_processing', 'data_loading'],  # Define tags for your DAG
   on_failure_callback = failure_sns,
   on_success_callback = success_sns
)
 
# Define your Task flow below ===========================================================
 
#Run python processing script
processing_script = BashOperator(
   task_id = 'Process_data',
   bash_command = (f"python3 {HOME_DIR}/moving_big_data/Scripts/data_processing_script.py"),
   dag = dag)
 
 # Define the task for uploading data to Postgres
upload_task = PythonOperator(
   task_id='upload_to_postgres',
   python_callable=upload_to_postgres,
   #provide_context=True,  # This allows passing context to the Python function
   dag=dag
)

# Set task dependencies
processing_script >> upload_task