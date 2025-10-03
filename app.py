#!/usr/bin/env python3
from aws_cdk import App
from cdk_stack.lambda_stack import IcebergSchemaEvolutionStack

app = App()
IcebergSchemaEvolutionStack(app, "IcebergSchemaEvolutionStack")
app.synth()

