import json
from typing import Optional

from lambda_helper import LambdaHelper
# data_bucket = "iceberg-schema-evolution-rel-030865023555"
# path = "assets/orders_v1.json"
# bucket = data_bucket
# lh: LambdaHelper = LambdaHelper(data_bucket=data_bucket, )
# lh.process_event(
#     path=path,
#     bucket=bucket,
#     run_local=not s3_file, )
# print(json.dumps(lh.process_schema_response.__dict__))
# print(lh.process_schema_response)


def handler():
    has_error: bool = False
    change_json: str = ""
    message_list: list[str] = []
    data_bucket = "iceberg-schema-evolution-rel-030865023555"
    try:
        file_name = "assets/orders_v1.json"
        s3_file: bool = False
        bucket: Optional[str] = data_bucket
        path: str = file_name


        lh: LambdaHelper = LambdaHelper(data_bucket=data_bucket, )
        lh.process_event(
            path=path,
            bucket=bucket,
            run_local=not s3_file, )
        change_json = json.dumps(lh.process_schema_response.__dict__)
        has_error = lh.process_schema_response.has_error
    except Exception as e:
        print(e)

    return {
        'statusCode': 500 if has_error else 200,
        'body': change_json,
    }

handler()