import boto3
import os
from dotenv import load_dotenv
import pandas as pd
import io

load_dotenv()
bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
region_name = os.getenv("AWS_REGION_NAME")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")


def upload_file_to_s3(file_name,key_name=None):
    s3_client = boto3.client('s3',region_name=region_name,aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    if not key_name:
        key_name = os.path.basename(file_name)
    try:
        s3_client.upload_file(file_name, bucket_name,key_name)
        print(f"File {file_name} uploaded to {bucket_name}")
    except FileExistsError:
        print(f"file{file_name} not found ")    



# lambda handler

if __name__ == "__main__":
    
    file_name = 'customers.csv'  
    upload_file_to_s3(file_name)
    
    