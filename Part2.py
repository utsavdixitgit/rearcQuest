import os
import boto3
import requests

bucketQuest = "rearcquestutsav"
populationData = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"


s3 = boto3.resource('s3',aws_access_key_id=os.getenv("aws_access_key_id", default=None),
         aws_secret_access_key= os.getenv("aws_secret_access_key", default=None))

bucket = s3.Bucket(bucketQuest)

# Request the API data and parse it
agent = {"User-Agent":"Mozilla/5.0"}
r = requests.get(populationData, headers=agent)
#r = requests.get(DATA_SOURCE)
data = r.text

# Upload the data to S3
bucket.put_object(Key="population.json", Body=data)
