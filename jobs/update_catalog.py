# update_catalog.py
import boto3

crawler_name = 'headlines-crawler'
glue = boto3.client('glue')

response = glue.start_crawler(Name=crawler_name)
print(f'Crawler {crawler_name} started: {response}')
