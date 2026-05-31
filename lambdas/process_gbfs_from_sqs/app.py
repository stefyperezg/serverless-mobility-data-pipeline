import os
import json
import boto3
import _utils_ as utils

SELECTED_STATION_IDS = utils.load_selected_stations()

def lambda_handler(event, context):
    s3_bucket = os.environ.get('S3_BUCKET')
    s3_prefix = os.environ.get('S3_PREFIX', 'gbfs_data/')
    s3 = boto3.client('s3')

    for record in event['Records']:
        raw_data = record['body']
        formatted_data = utils.parse_gbfs(raw_data)
        filtered_data = utils.format_filter_stations(formatted_data, SELECTED_STATION_IDS)
        key = f"{s3_prefix}gbfs_{context.aws_request_id}.json"
        s3.put_object(
            Bucket=s3_bucket, 
            Key=key, 
            Body=json.dumps(filtered_data))

    return {
        'statusCode': 200,
        'body': json.dumps('GBFS data processed and saved to S3')
    }
