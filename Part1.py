import os

import boto3
import requests
from bs4 import BeautifulSoup

bucketQuest = "rearcquestutsav"
timeSeries = "https://download.bls.gov/pub/time.series/pr/"

s3 = boto3.resource('s3',aws_access_key_id=os.getenv("aws_access_key_id", default=None),
         aws_secret_access_key= os.getenv("aws_secret_access_key", default=None))

bucket = s3.Bucket(bucketQuest)

# Get bucket contents
bucket_objects = []

for obj in bucket.objects.all():
    bucket_objects.append(obj.key)

# Request the data source and parse it
agent = {"User-Agent":"Mozilla/5.0"}
r = requests.get(timeSeries, headers=agent)
soup = BeautifulSoup(r.text, 'html.parser')

for link in soup.find_all("a"):
    # Download the current file
    file_name = link.get_text()

    if file_name == "[To Parent Directory]":
        continue
    file_dl = requests.get(timeSeries + file_name, headers=agent)

    # If the file doesn't exist in S3, upload it
    if file_name not in bucket_objects:
        bucket.put_object(Key=file_name, Body=file_dl.content)
    # If the file exists in S3
    elif file_name in bucket_objects:
        # Get the S3 file
        s3_response = bucket.Object(file_name).get()
        s3_file_content = s3_response['Body'].read()
        # If the S3 file is different from the website file, update the S3 file
        if file_dl.content != s3_file_content:
            bucket.put_object(Key=file_name, Body=file_dl.content)

