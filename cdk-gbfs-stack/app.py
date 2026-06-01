#!/usr/bin/env python3
from dotenv import load_dotenv
import os
import aws_cdk as cdk
from gbfs_stack import GbfsStack

load_dotenv()

required_vars = ["GBFS_URL", "S3_PREFIX", "CITY_ID", "S3_BUCKET"]

for v in required_vars:
    if not os.environ.get(v):
        raise ValueError(f"Missing required env var: {v}")

app = cdk.App()
GbfsStack(app, "GbfsStack")
app.synth()
