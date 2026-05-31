import os
import json
import boto3
import requests

def lambda_handler(event, context):
    gbfs_url = os.environ.get('GBFS_URL')
    sqs_queue_url = os.environ.get('QUEUE_URL')

    all_data_links = requests.get(gbfs_url).json()
    feeds_dict = {feed['name']: feed['url'] for feed in all_data_links['data']['en']['feeds']}
    request_url = feeds_dict['station_status']

    station_status_res = requests.get(request_url)

    station_status_res.raise_for_status()
    data = station_status_res.text  # keep raw

    sqs = boto3.client('sqs')
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=data)

    return {
        'statusCode': 200,
        'body': json.dumps('GBFS data sent to SQS')
    }

lambda_handler(None, None)