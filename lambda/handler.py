import json
import logging
import os
from typing import Optional
from urllib.parse import urlparse

from process_schema_response import ProcessSchemaResponse
from data_generator import GenerateData
from lambda_helper import LambdaHelper

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info(json.dumps(event, indent=2))
    logger.info(context)
    has_error: bool = False
    change_json: str = ""
    message_list: list[str] = []
    logger.info("Received event: " + json.dumps(event, indent=2))
    data_bucket: str = os.environ.get("DATA_BUCKET", None)
    try:
        file_name: str = event.get("file_name", None)
        parsed_uri = urlparse(file_name)
        s3_file: bool = parsed_uri.scheme == 's3'
        bucket: Optional[str] = parsed_uri.netloc if s3_file else None
        path: str = parsed_uri.path

        has_error = data_bucket is None or file_name is None or not data_bucket or not file_name
        logger.info(
            f"Bucket: {bucket}. Path: {path}. has_error: {has_error}. DataBucket: {data_bucket}. FileName: {file_name}")
        if file_name is None:
            message_list.append(f"file_name not specified in input event")

        if not has_error:
            lh: LambdaHelper = LambdaHelper(data_bucket=data_bucket, )
            lh.process_event(
                path=path,
                bucket=bucket,
                run_local=not s3_file, )
            change_json = json.dumps(lh.process_schema_response.__dict__)
            has_error = lh.process_schema_response.has_error
            if not lh.process_schema_response.has_error:
                if path == "assets/orders_v1.json" or path == "assets/orders_v2.json":
                    version:str = "v1" if path.__contains__("1") else "v2"
                    dg:GenerateData = GenerateData()
                    dg.insert_order(lh.process_schema_response.database_name, lh.process_schema_response.table_name,version)
            return {
                'statusCode': 500 if has_error else 200,
                'body': change_json,
            }
        else:
            logger.error(message_list)
            return {
                'statusCode': 500 if has_error else 200,
                'body': ",\n".join(message_list),
            }
    except Exception as e:
        logger.error(e)
        return {
            'statusCode': 500 if has_error else 200,
            'body': change_json,
        }
