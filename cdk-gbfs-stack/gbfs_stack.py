import os

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_events as events,
    aws_events_targets as targets,
    aws_glue as glue,
    RemovalPolicy,
    Duration
)
from constructs import Construct

class GbfsStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # S3 bucket with retention
        bucket = s3.Bucket(self, "GbfsDataBucket",
            bucket_name=os.environ.get("S3_BUCKET"), 
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False
        )

        # SQS queue
        queue = sqs.Queue(self, "GbfsRawDataQueue")

        # Lambda: fetch_gbfs_to_sqs (container image)
        fetch_lambda = _lambda.DockerImageFunction(
            self, "FetchGbfsToSqsFunction",
            code=_lambda.DockerImageCode.from_image_asset("../lambdas/fetch_gbfs_to_sqs"),
            environment={
                "GBFS_URL": os.environ.get("GBFS_URL"),
                "QUEUE_URL": queue.queue_url
            },
            timeout=Duration.seconds(30),
            memory_size=256
        )
        queue.grant_send_messages(fetch_lambda)

        # Lambda: process_gbfs_from_sqs (container image)
        process_lambda = _lambda.DockerImageFunction(
            self, "ProcessGbfsFromSqsFunction",
            code=_lambda.DockerImageCode.from_image_asset("../lambdas/process_gbfs_from_sqs"),
            environment={
                "S3_BUCKET": bucket.bucket_name,
                "S3_PREFIX": os.environ.get("S3_PREFIX", "gbfs/"),
                "CITY_ID": os.environ.get("CITY_ID", "unknown_city"),
                "SELECTED_STATIONS_FILE": os.environ.get("SELECTED_STATIONS_FILE", "selected_stations.csv")   
            },
            timeout=Duration.seconds(30),
            memory_size=256
        )
        bucket.grant_write(process_lambda)
        queue.grant_consume_messages(process_lambda)
        process_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(queue)
        )

        # EventBridge rule to run fetch_lambda every hour
        rule = events.Rule(self, "HourlyGbfsFetchRule",
            schedule=events.Schedule.rate(Duration.hours(1))
        )
        rule.add_target(targets.LambdaFunction(fetch_lambda))

        db = glue.CfnDatabase(
            self, "GbfsDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=os.environ.get("GLUE_DATABASE_NAME")
            )
        )

        table = glue.CfnTable(
            self, "GbfsStationStatusTable",
            catalog_id=self.account,
            database_name=db.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="gbfs_station_status",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet"
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="status_uuid", type="string"),
                        glue.CfnTable.ColumnProperty(name="city", type="string"),
                        glue.CfnTable.ColumnProperty(name="station_id", type="int"),
                        glue.CfnTable.ColumnProperty(name="num_bikes_available", type="int"),
                        glue.CfnTable.ColumnProperty(name="num_docks_available", type="int"),
                        glue.CfnTable.ColumnProperty(name="is_renting", type="boolean"),
                        glue.CfnTable.ColumnProperty(name="is_returning", type="boolean"),
                        glue.CfnTable.ColumnProperty(name="last_reported", type="bigint")
                    ],
                    location=f"s3://{bucket.bucket_name}/{os.environ.get('S3_PREFIX', 'gbfs/')}",
                    
                    input_format="org.apache.hadoop.hibe.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    )
                )   
            ),
            partition_keys=[
                glue.CfnTable.ColumnProperty(name="city", type="string"),
                glue.CfnTable.ColumnProperty(name="year", type="int"),
                glue.CfnTable.ColumnProperty(name="month", type="int"),
                glue.CfnTable.ColumnProperty(name="day", type="int"),
                glue.CfnTable.ColumnProperty(name="hour", type="int")
            ]
        )   

        # Outputs
        self.bucket = bucket
        self.queue = queue
        self.fetch_lambda = fetch_lambda
        self.process_lambda = process_lambda
