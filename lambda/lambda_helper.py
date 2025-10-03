import json
import logging
import os
import uuid
from dataclasses import asdict
from typing import Optional
from urllib.parse import urlparse

import boto3
from jsonschema import validate, ValidationError

from process_schema_response import ProcessSchemaResponse
from iceberg_helper import IcebergHelper

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class LambdaHelper:

    def __init__(self, data_bucket: str):
        self.data_bucket = data_bucket
        self.bucket = data_bucket
        self.has_error = False
        self.message_list = []
        self.process_schema_response = ProcessSchemaResponse()

    def process_event(self, path: str, bucket: Optional[str] = None, run_local: bool = False):
        logger.info(f"Processing event {path}. Run local: {run_local}. bucket: {bucket}")
        def_loaded, table_def_dict, messages = self._load_table_def(bucket, path, run_local)
        if def_loaded:
            logger.info("loaded table definition")
            logger.info(table_def_dict)
            self.process_schema_response.database_name = table_def_dict["database_name"]
            self.process_schema_response.table_name = table_def_dict["table_name"]
            ih: IcebergHelper = IcebergHelper(bucket=self.data_bucket,process_change_response=self.process_schema_response)
            ih.process_table(table_def_dict)
            self._save_output()
        else:
            logger.error("failed to load table definition")
            logger.error(messages)
            self.process_schema_response.has_error = True
            self.process_schema_response.message_list.append("Failed to load table definition")


    def _validate_schema(self, schema_def_path: str, table_def_path: str) -> (bool, list[str]):
        """
        Validates a table definition against schema definition.
        :param schema_def_path: Path to the schema definition. This can be either a local for unit tests or S3 prefix
        :param table_def_path: Path to the table definition. This can be either a local for unit tests or S3 prefix
        :return: True if schema validation passes, otherwise False. Returns a list of validation errors.
        """
        is_valid = False
        messages = []
        schema_file_found = os.path.isfile(schema_def_path)
        table_def_file_found = os.path.isfile(table_def_path)
        if not schema_file_found or not table_def_file_found:
            logger.error(
                f"Schema definition file not found: {schema_def_path} or Table definition file not found: {table_def_path}")
        if schema_file_found and table_def_file_found:
            is_valid = True
            try:
                with open(schema_def_path, "r",encoding="utf-8") as f:
                    schema = json.load(f)
                with open(table_def_path, 'r',encoding="utf-8") as f:
                    table_def = json.load(f)
                validate(instance=table_def, schema=schema)
                logger.info("JSON is valid against the schema.")
                is_valid = True
            except ValidationError as e:
                logger.error(f"JSON validation error: {e.message}")
                messages.append(f"JSON validation error: {e.message}")
                self.process_schema_response.message_list.append(f"JSON validation error: {e.message}")
                self.process_schema_response.has_error = True
            except Exception as ex:
                logger.error(f"An unexpected error occurred: {ex}")
                messages.append(f"An unexpected error occurred: {ex}")
                self.process_schema_response.message_list.append(f"An unexpected error occurred: {ex}")
                self.process_schema_response.has_error = True
        return is_valid, messages

    def _s3_load_table_def(self,bucket:str, path: str) -> (bool, dict):

        table_def_dict: dict = {}
        try:
            prefix:str = path.lstrip("/")
            logger.info(f"Loading table definition for {bucket}  and {prefix}")
            # Initialize the S3 resource and get and read S3 object
            s3 = boto3.resource('s3')
            s3_object = s3.Object(bucket, prefix)
            file_content = s3_object.get()['Body'].read().decode('utf-8')
            # Parse the JSON content
            table_def_dict = json.loads(file_content)
            is_loaded = True
        except Exception as e:
            is_loaded = False
            logger.error(f"Error loading JSON from S3: {e}")
            self.process_schema_response.message_list.append(f"Error loading JSON from S3: {e}")
            self.process_schema_response.has_error = True
        return is_loaded, table_def_dict

    def _local_load_table_def(self, table_def_path: str) -> (bool, dict):
        table_def_dict: dict = {}
        try:
            with open(table_def_path, 'r',encoding="utf-8") as f:
                table_def_dict = json.load(f)
            is_loaded = True
            logger.info("JSON is valid against the schema.")
        except FileNotFoundError:
            self.process_schema_response.message_list.append(f"Table definition file not found: {table_def_path}")
            self.process_schema_response.has_error = True
            is_loaded = False

        return is_loaded, table_def_dict

    def _load_table_def(self, bucket: str, path: str, run_local: bool = False) -> (bool, dict, list[str]):
        """
        Validates a table definition against schema definition.
        :param bucket: Bucket to read config from.
        :param path: Path/Prefix to the table definition. This can be either a local for unit tests or S3 prefix
        :param run_local: Default is False i.e. read from S3.
        :return: True if schema validation passes, otherwise False. Returns a list of validation errors.
        """
        is_loaded = False
        table_def_dict: dict = {}
        messages = []
        table_def_file_found = os.path.isfile(path) if run_local else True
        if not table_def_file_found:
            logger.error(
                f"Table definition file not found: {path}")
            self.process_schema_response.message_list.append(f"Table definition file not found: {path}")
            self.process_schema_response.has_error = True
        if table_def_file_found:
            is_loaded, table_def_dict = self._local_load_table_def(
                path) if run_local else self._s3_load_table_def(bucket,path)

        return is_loaded, table_def_dict, messages

    def _save_output(self):
        """
          Saves a Python dataclass instance to S3 as a JSON file.

          """
        # Convert dataclass to dictionary, then to JSON string
        s3_key = f"output/{str(uuid.uuid4())}.json"
        self.process_schema_response.output_location = f"s3://{self.bucket}/{s3_key}"
        data_dict = asdict(self.process_schema_response)
        json_string = json.dumps(data_dict, indent=4)  # indent for readability

        # Upload to S3
        s3 = boto3.client('s3')
        try:
            s3.put_object(Bucket=self.bucket, Key=s3_key, Body=json_string)
            logger.info(f"Successfully saved output to s3://{self.bucket}/{s3_key}")
            self.process_schema_response.output_location= f"s3://{self.bucket}/{s3_key}"
        except Exception as e:
            self.process_schema_response.message_list.append(f"Failed to save output to s3: {e}")
            logger.error(f"Error saving dataclass to S3: {e}")
            self.process_schema_response.output_location = f"Error saving dataclass to S3: {e}"
            return f"Error saving output to S3: {e}"
