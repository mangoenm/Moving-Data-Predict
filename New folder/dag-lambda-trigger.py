import requests
import json
#import datetime
import boto3

airflow_instance_ip = "ec2-52-211-66-86.eu-west-1.compute.amazonaws.com"
airflow_dag = "NAOMAN_DAG"

def lambda_handler(event, context):
        
    #print(airflow_instance_ip)
    #print(airflow_dag)

        # Initialize the S3 client
    s3_client = boto3.client('s3')

    # Check if the .SUCCESS file exists in the S3 bucket
    try:
        s3_client.head_object(Bucket='2301ft-mbd-predict-naomi-mangoejane-monitored-bucket', Key='TEST/my_test.SUCCESS')
    except s3_client.exceptions.NoSuchKey:
        print(f"The .SUCCESS file 'TEST/my_test.SUCCESS' does not exist in bucket '2301ft-mbd-predict-naomi-mangoejane-monitored-bucket'. Exiting.")
        return {
            'statusCode': 200,
            'body': 'SUCCESS file not found. No action taken.'
        }

    # Create the content for the .SUCCESS file
    success_content = "Success!\n"
    
    response = requests.post(f"http://ec2-52-211-66-86.eu-west-1.compute.amazon.com:8080/api/v1/dags/NAOMAN_DAG/dagRuns",
                             headers={"Content-Type": "application/json", "Accept": "application/json"},
                             auth = ("airflow", "airflow"),
                             json={"conf": {}})

    print('event', event)
    print('context', context)

    if response.status_code == 200:
        print("DAG triggered successfully")
    else:
        print(f"Failed to trigger the DAG, status code: {response.status_code}")
        raise Exception("Failed to trigger the DAG")