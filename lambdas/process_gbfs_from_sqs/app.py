import os
import json
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timezone
import _utils_ as utils

SELECTED_STATION_IDS = utils.load_selected_stations()

def lambda_handler(event, context):
    s3_bucket = os.environ.get('S3_BUCKET')
    s3_prefix = os.environ.get('S3_PREFIX', 'gbfs_data/')
    city = os.environ.get('CITY_ID', 'unknown_city')
    s3 = boto3.client('s3')

    for record in event['Records']:
        raw_data = record['body']

        formatted_data = utils.parse_gbfs(raw_data)
        filtered_data = utils.format_filter_stations(formatted_data, SELECTED_STATION_IDS)
        
        # timestamp for partitioning
        now = datetime.now(timezone.utc)
        year = now.year
        month = now.month
        day = now.day

        #convert to parquet
        table = pa.Table.from_pylist(filtered_data)
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        key = (
            f"{s3_prefix}/city={city}/"
            f"year={year}/month={month}/day={day}/"
            f"{context.aws_request_id}.parquet"
            )
        s3.put_object(
            Bucket=s3_bucket, 
            Key=key, 
            Body=parquet_buffer.getvalue()
        )

    return {
        'statusCode': 200,
        'body': json.dumps('GBFS data processed and saved to S3')
    }
