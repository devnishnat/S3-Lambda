import boto3
import pandas as pd
import io
import psycopg2
from psycopg2 import sql
import os
def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    try:
        # bucket = event['Records'][0]['s3']['bucket']['name']
        # key = event['Records'][0]['s3']['object']['key'] 
        bucket = 'dev.test01'
        key = 'customers.csv'  # Replace with your S3 bucket and key

        obj =s3_client.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read()

        df = pd.read_csv(io.BytesIO(data))

        print(df)
        
        conn = psycopg2.connect(

            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )

        cursor = conn.cursor()

        for index, row in df.iterrows():
            full_name = f"{row['First Name']} {row['Last Name']}"
            cursor.execute("""
                INSERT INTO customers (id, name, email, subscription_date, website)
                VALUES (%s, %s, %s, %s, %s)
                """, (
                    row['Customer Id'],
                    full_name,
                    row['Email'],
                    row['Subscription Date'],
                    row['Website']
                )
            )

        conn.commit()
        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': 'data stored successfully'
        }

    except Exception as e:
        print(f"Error processing file: {e}")
        return {
            'statusCode': 500,
            'body': f"Error processing file: {e}"
        }


lambda_handler("", "")
