import boto3
import os

# S3 bucket information
bucket_name = 'uber-raw-data-naveen'
s3_file_path = 'uber.csv'  # Destination path in S3

# Local file path
local_file_path = 'uber.csv'  # Replace with your local file path

# Upload file to S3
try:
    # Create S3 client
    s3 = boto3.client('s3', aws_access_key_id='*********', aws_secret_access_key='********')
    
    # Upload a file
    s3.upload_file(local_file_path, bucket_name, s3_file_path)
    
    print(f"File {local_file_path} uploaded to S3 bucket {bucket_name} at {s3_file_path}")

except Exception as e:
    print(f"Error occurred: {e}")
