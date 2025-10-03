# Iceberg Schema Evolution

A serverless AWS solution for managing Apache Iceberg table schema evolution using AWS Lambda, deployed with AWS CDK.

## Overview

This project demonstrates how to handle schema evolution in Apache Iceberg tables through a serverless architecture. It provides automated schema processing, data generation, and evolution management capabilities.

## Architecture

- **AWS Lambda**: Containerized Python functions for schema processing
- **AWS CDK**: Infrastructure as Code for deployment
- **Apache Iceberg**: Table format with schema evolution support
- **Docker**: Containerized Lambda deployment

## Features

- Schema evolution processing and validation
- Automated data generation for testing
- Iceberg table management utilities
- Serverless architecture with AWS Lambda
- Infrastructure deployment with CDK

## Prerequisites

- Python 3.12+
- AWS CLI configured
- AWS CDK installed
- Docker (for Lambda container builds)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd iceberg-schema-evolution
```

2. Deploy solution. This command will
   1. Setup virtual environment
   2. Install dependencies
   3. Deploy the infrastructure using CDK:
```bash
chmod +x install_env.sh
./install_env.sh
```

## Project Structure

```
├── app.py                 # CDK app entry point
├── cdk_stack/            # CDK stack definitions
│   └── lambda_stack.py   # Lambda infrastructure
├── lambda/               # Lambda function code
│   ├── handler.py        # Main Lambda handler
│   ├── iceberg_helper.py # Iceberg utilities
│   ├── data_generator.py # Test data generation
│   ├── Dockerfile        # Lambda container
│   └── requirements.txt  # Lambda dependencies
└── README.md
```

## Usage

The Lambda function processes schema evolution events and manages Iceberg table operations. It can be triggered through various AWS services or invoked directly.

## Development

To modify the Lambda function:

1. Update code in the `lambda/` directory
2. Rebuild and redeploy:
```bash
cdk deploy
```

## Environment Variables

The Lambda function uses the following environment variables:
- `DATA_BUCKET`: S3 bucket for data storage

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request
