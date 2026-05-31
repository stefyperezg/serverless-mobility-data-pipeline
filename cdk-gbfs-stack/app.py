#!/usr/bin/env python3
import aws_cdk as cdk
from gbfs_stack import GbfsStack

app = cdk.App()
GbfsStack(app, "GbfsStack")
app.synth()
