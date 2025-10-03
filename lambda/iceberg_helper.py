import json
import logging
import os
import uuid
from dataclasses import dataclass, asdict
from typing import Optional

import boto3
import pyiceberg.exceptions
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table, Transaction
from pyiceberg.transforms import DayTransform, IdentityTransform, MonthTransform, YearTransform, HourTransform
from pyiceberg.types import NestedField, StructType, MapType, ListType, IcebergType

from process_schema_response import ProcessSchemaResponse
from constants import IcebergDataType, map_config_type_to_iceberg_type

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class SchemaChange:
    """
    Holds the schema change information
    """
    column_name: str = None
    data_type: str = None
    existing_column: bool = False
    add: bool = False
    drop: bool = False
    update: bool = False
    is_primitive: bool = False
    is_root_column: bool = False
    is_nested_column: bool = False
    root_column: str = None
    parent_column: str = None
    nested_level: int = 1
    new_ns: NestedField = None
    existing_ns: NestedField = None
    _field_id: int = -1
    _is_struct: bool = False
    disallowed: bool = False
    disallowed_message: list[str] = None

    def data_type_change_allowed(self) -> (bool, list[str]):
        logger.debug(f"DataType change allowed: {self.data_type}")
        change_allowed = True
        message_list: list[str] = []
        if not self.drop and not self.add:
            if self.drop == False and (self.existing_ns is None or self.new_ns is None):
                message_list.append("Existing or new NestedField is None")
                change_allowed = False
            else:
                if not self.existing_ns.field_type.is_primitive == self.new_ns.field_type.is_primitive:
                    existing_type = type(self.existing_ns.field_type).__name__
                    new_type = type(self.new_ns.field_type).__name__
                    message_list.append(
                        f"Field: {self.column_name}. Cannot change from: {existing_type} to {new_type}. ")
                    change_allowed = False
        self.disallowed = not change_allowed
        self.disallowed_message = message_list
        return change_allowed, message_list

    def __post_init__(self):
        self.root_column = self.column_name.split(".")[0]
        self.is_root_column = self.root_column == self.column_name
        self.is_nested_column = self.root_column != self.column_name
        self.is_primitive = False if self.new_ns is None else self.new_ns.field_type.is_primitive
        self.parent_column = ".".join(self.column_name.split(".")[:-1])
        self.nested_level = len(self.column_name.split("."))
        self._is_struct = False if self.new_ns is None else self.new_ns.field_type.is_struct
        self._field_id = -1 if self.new_ns is None else self.new_ns.field_id


class IcebergHelper:
    WAREHOUSE_PATH: str = "/tmp/pyiceberg_warehouse"

    def _table_location(self, catalog: Catalog, database_name: str, table_name: str) -> str:
        """
        Based on catalog type gets the location of a table.
        :param catalog:
        :param database_name:
        :param table_name:
        :return:
        """
        match catalog.properties.get("type"):
            case "glue":
                return f"s3://{os.path.join(self.bucket, database_name, table_name)}"
            case "sql":
                return os.path.join(catalog.properties.get("warehouse"), database_name, table_name)
            case _:
                return ""

    def __init__(self, catalog: Catalog = None, bucket: str = None,process_change_response: ProcessSchemaResponse = None):
        """
        Sets up catalog and local_catalog. Local catalog is used to load the schema definition locally and is a sql lite
        catalog.
        :param catalog:
        """
        self.bucket = bucket
        if catalog is None:
            self.catalog = load_catalog(
                "glue",
                **{
                    "type": "glue",
                },
            )
        else:
            self.catalog = catalog
        os.makedirs(self.WAREHOUSE_PATH, exist_ok=True)
        if catalog is None:
            self.local_catalog = load_catalog(
                "default",
                **{
                    "type": "sql",
                    "uri": f"sqlite:///{self.WAREHOUSE_PATH}/pyiceberg_catalog.db",
                    "warehouse": f"file://{self.WAREHOUSE_PATH}",
                },
            )
        else:
            self.local_catalog = catalog
        self.process_change_response = process_change_response

    def _get_nested_field(self, column_def: Optional[dict]) -> Optional[NestedField]:
        """
        Accepts a column definition and recursively converts it to a NestedField
        :param column_def:
        :return:
        """
        column_name = column_def.get('column_name', str(uuid.uuid4()))
        data_type = column_def.get('data_type')
        ice_data_type: IcebergDataType = map_config_type_to_iceberg_type(data_type)
        required_type = column_def.get('required', False)
        # This is return for a non nested primitive data field.
        if not ice_data_type.is_nested_type:
            return NestedField(field_id=-1, name=column_name, field_type=ice_data_type.native_iceberg_type,
                               required=required_type)
        else:
            match ice_data_type:
                case IcebergDataType.STRUCT_TYPE:
                    nf_list: list[NestedField] = []
                    struct_def: list[dict] = column_def.get('struct_def')
                    if struct_def:
                        for field in struct_def:
                            nf_list.append(self._get_nested_field(field))
                        struct_ns: NestedField = NestedField(field_id=-1, name=column_name,
                                                             required=required_type,
                                                             field_type=StructType(*nf_list))

                        return struct_ns
                    else:
                        return None
                case IcebergDataType.MAP_TYPE:
                    map_def: dict = column_def.get('map_def')
                    if map_def:
                        ns_key_type: NestedField = self._get_nested_field(map_def.get('key', None))
                        ns_value_type = self._get_nested_field(map_def.get('value', None))
                        map_ns: NestedField = NestedField(field_id=-1, name=column_name,
                                                          field_type=MapType(key_id=-1,
                                                                             value_id=-1,
                                                                             key_type=ns_key_type.field_type,
                                                                             value_type=ns_value_type.field_type),
                                                          required=required_type)
                        return map_ns
                    else:
                        return None
                case IcebergDataType.ARRAY_TYPE | IcebergDataType.LIST_TYPE:
                    ns_list_type: NestedField = self._get_nested_field(column_def.get('array_def', None))
                    list_ns: NestedField = NestedField(field_id=-1, name=column_name, required=required_type,
                                                       field_type=ListType(element_id=-1,
                                                                           element_type=ns_list_type.field_type,
                                                                           element_required=required_type, ))
                    return list_ns
                case _:
                    return None

    def load_table(self, database_name: str, table_name: str) -> Optional[Table]:
        try:
            return self.catalog.load_table(f"{database_name}.{table_name}")
        except pyiceberg.exceptions.NoSuchTableError as no_such_table:
            logger.error(no_such_table)
            return None

    def _load_schema_locally(self, database_name: str, table_name: str, input_schema: Schema) -> Schema:
        self.local_catalog.create_namespace_if_not_exists(database_name)
        if self.local_catalog.table_exists(f"{database_name}.{table_name}"):
            self.local_catalog.drop_table(f"{database_name}.{table_name}")
        local_table: Table = self.local_catalog.create_table(
            identifier=f"{database_name}.{table_name}",
            schema=input_schema,
            location=self._table_location(self.local_catalog, database_name, table_name), )
        return local_table.schema()

    def process_table(self, table_def_dict: dict):
        table_name: str = table_def_dict.get('table_name')
        database_name: str = table_def_dict.get('database_name')
        try:
            columns: list = table_def_dict.get('columns')
            column_definitions: dict[str, NestedField] = {}
            if columns:
                for c in range(len(columns)):
                    column_name = columns[c].get('column_name')
                    data_type: str = columns[c].get('data_type')
                    ns: NestedField = self._get_nested_field(columns[c])
                    column_definitions[column_name] = ns

            schema_field_list: list[NestedField] = list(column_definitions.values())
            schema: Schema = Schema(*schema_field_list)
            target_schema: Schema = self._load_schema_locally(database_name, table_name, schema)
            identifier = f"{database_name}.{table_name}"
            if self.catalog.table_exists(identifier):
                self._update_table(table_def_dict, target_schema)
            else:
                self._create_table(table_def_dict, target_schema)
        except Exception as ex:
            self.process_change_response.has_error = True
            self.process_change_response.message_list.append(f"Failed to create table {table_name}")
            self.process_change_response.message_list.append(str(ex))
            logger.error(ex)



    def _create_table(self, table_def_dict: dict, target_schema: Schema) :
        table_name: str = table_def_dict.get('table_name')
        database_name: str = table_def_dict.get('database_name')
        try:
            self.catalog.create_namespace_if_not_exists(database_name)
            identifier = f"{database_name}.{table_name}"
            partition_spec: PartitionSpec = self._get_partition_spec(table_def_dict, target_schema)
            self.process_change_response.change_type = "CREATE TABLE"
            final_table: Table = self.catalog.create_table(
                identifier=identifier,
                schema=target_schema,
                location=self._table_location(self.catalog, database_name, table_name),
                partition_spec=partition_spec,
            )
            self.process_change_response.message_list.append(f"Created table {str.join(".", final_table.name())}")
            self.process_change_response.message_list.append(f"Latest Meta file: {final_table.metadata_location}")
        except Exception as ex:
            self.process_change_response.has_error = True
            self.process_change_response.message_list.append(f"Failed to create table {table_name}")
            logger.error(ex)

    def _update_table(self, table_def_dict: dict, target_schema: Schema, ) :
        table_name: str = table_def_dict.get('table_name')
        database_name: str = table_def_dict.get('database_name')
        identifier = f"{database_name}.{table_name}"
        self.catalog.create_namespace_if_not_exists(database_name)
        try:
            self.process_change_response.change_type = "ALTER TABLE"
            schema_change_list: list[SchemaChange] = []

            existing_table: Table = self.load_table(database_name, table_name)

            col_type_map: dict[str, IcebergType] = {}

            for c in range(len(target_schema.column_names)):
                ns: NestedField = target_schema.find_field(target_schema.column_names[c])
                col_type_map[target_schema.column_names[c]] = ns.field_type
                logger.debug(f"{target_schema.column_names[c]} is {ns.field_type.is_struct}")

            for i in range(len(target_schema.column_names)):
                column_name: str = target_schema.column_names[i]
                sc: SchemaChange = SchemaChange(
                    column_name=column_name,
                    new_ns=target_schema.find_field(column_name),
                )
                schema_change_list.append(sc)

            for i in range(len(existing_table.schema().column_names)):
                column_name: str = existing_table.schema().column_names[i]
                # see if column exists
                keep_column = list(filter(lambda kc: kc.column_name == column_name, schema_change_list))
                if not keep_column:
                    # if not keep column set drop = true
                    sc: SchemaChange = SchemaChange(
                        column_name=column_name,
                        drop=True
                    )
                    schema_change_list.append(sc)
                else:
                    field_type = existing_table.schema().find_field(column_name).field_type
                    sc: SchemaChange = keep_column[0]
                    sc.existing_column = True
                    sc.existing_ns = existing_table.schema().find_field(column_name)
                    sc.update = sc.new_ns.field_type != field_type

            add_column_list: list[SchemaChange] = list(
                filter(lambda ac: ac.existing_column == False, schema_change_list))
            if add_column_list:
                for add_column in add_column_list:
                    add_column.add = True

            for c in schema_change_list:
                change_allowed, message = c.data_type_change_allowed()
                logger.debug(message)
            partition_spec: PartitionSpec = self._get_partition_spec(table_def_dict, target_schema)

            try:
                with existing_table.update_schema(allow_incompatible_changes=False) as update_schema:
                    # with schema_txn.update_schema() as update_schema:
                    # drop fields
                    drop_field_list: list = list(filter(lambda field: field.drop == True, schema_change_list))
                    if drop_field_list:
                        for d in drop_field_list:
                            update_schema.delete_column(d.column_name)
                            self.process_change_response.message_list.append(f"Dropped column {d.column_name}")
                    root_add_field_list: list = list(
                        filter(lambda field: field.add == True and field.is_root_column == True, schema_change_list))
                    if root_add_field_list:
                        for a in root_add_field_list:
                            # now update all fields with root field = this added field
                            child_field_list: list = list(filter(lambda
                                                                     child: child.add == True and child.is_root_column == False and child.root_column == a.column_name,
                                                                 schema_change_list))
                            if child_field_list:
                                for c in child_field_list:
                                    c.add = False
                            update_schema.add_column(a.column_name, field_type=a.new_ns.field_type)
                            self.process_change_response.message_list.append(f"Added column {a.column_name}")
                    non_root_child_field_list: list = list(
                        filter(lambda field: field.add == True and field.is_root_column == False, schema_change_list))
                    if non_root_child_field_list:
                        for n in non_root_child_field_list:
                            update_schema.add_column(tuple(n.column_name.split(".")), field_type=n.new_ns.field_type)
                            self.process_change_response.message_list.append(f"Added column {n.column_name}")
                    # we will only update primitive fields. Not nested types.
                    update_field_list: list = list(
                        filter(lambda field: field.update == True and field.is_primitive == True, schema_change_list))
                    if update_field_list:
                        for u in update_field_list:
                            update_schema.update_column(path=u.column_name, field_type=u.new_ns.field_type)
                            self.process_change_response.message_list.append(f"Updated column {u.column_name}")

                    root_field_list: list = list(
                        filter(lambda field: field.is_root_column == True and field.drop == False, schema_change_list))
                    if root_field_list:
                        sorted_by_field_id = sorted(root_field_list, key=lambda f: f._field_id)
                        for r in range(len(sorted_by_field_id)):
                            if r == 0:
                                update_schema.move_first(sorted_by_field_id[r].column_name)
                            else:
                                update_schema.move_after(sorted_by_field_id[r].column_name,
                                                         sorted_by_field_id[r - 1].column_name)

                    # now arrange all the struct fields.
                    struct_field_list: list = list(
                        filter(lambda field: field._is_struct == True and field.drop == False, schema_change_list))
                    if struct_field_list:
                        for s in struct_field_list:
                            child_field_list: list = sorted(
                                list(filter(lambda mcf: mcf.parent_column == s.column_name, schema_change_list)),
                                key=lambda f: f._field_id)
                            if child_field_list:
                                for cf in range(len(child_field_list)):
                                    if cf == 0:
                                        update_schema.move_first(child_field_list[cf].column_name)
                                    else:
                                        update_schema.move_after(child_field_list[cf].column_name,
                                                                 child_field_list[cf - 1].column_name)
                # Refresh the table to get latest schema and field_ids
                existing_table.refresh()
                partition_spec: PartitionSpec = self._get_partition_spec(table_def_dict, existing_table.schema())
                # check to see if there is a change in partition by comparing the fields.
                partition_changed: bool = not partition_spec.fields == existing_table.spec().fields
                if partition_changed:
                    # we will remove the old fields and add the new fields.
                    with existing_table.update_spec(case_sensitive=True) as update_spec:
                        for field in existing_table.spec().fields:
                            update_spec.remove_field(field.name)
                        partition_index:int = 0
                        for field in partition_spec.fields:
                            partition_field_name:str = self._get_partition_def(table_def_dict,partition_index)
                            update_spec.add_field(source_column_name=partition_field_name,transform=field.transform,partition_field_name=field.name)
                            partition_index += 1
            except Exception as e:
                self.process_change_response.has_error = True
                self.process_change_response.message_list.append(f"Failed to modify table: {identifier}")
                logger.error(e)

            existing_table.refresh()
            self.process_change_response.message_list.append(f"Latest Meta file: {existing_table.metadata_location}")
        except Exception as e:
            self.process_change_response.has_error = True
            self.process_change_response.message_list.append(f"Failed to modify table: {identifier}")
            logger.error(e)

    def _get_partition_def(self,table_def_dict:dict, partition_index:int) -> str or None:
        partition_list: list[dict] = table_def_dict.get('partitions', [])
        if partition_list and len(partition_list) > partition_index:
            return partition_list[partition_index].get('column',None)
        else:
            return None

    def _get_partition_spec(self, table_def_dict: dict, target_schema: Schema) -> PartitionSpec:
        partitions: list[dict] = table_def_dict.get('partitions', [])
        partition_field_list: list[PartitionField] = []
        partition_field_id: int = 1000
        if partitions:
            for partition in partitions:
                column: str = partition.get('column')
                partition_source: NestedField = target_schema.find_field(column)
                source_id: int = partition_source.field_id
                transform: str = partition.get('transform', 'identity')
                partition_name: str = partition.get('name', f"{column}_{transform}")
                match transform:
                    case "hour":
                        transform_by = HourTransform()
                    case "day":
                        transform_by = DayTransform()
                    case "year":
                        transform_by = YearTransform()
                    case "month":
                        transform_by = MonthTransform()
                    case _:
                        transform_by = IdentityTransform()

                partition_field_list.append(PartitionField(
                    source_id=source_id, field_id=partition_field_id, transform=transform_by, name=partition_name
                ))
                partition_field_id = partition_field_id + 1
        return PartitionSpec(fields=partition_field_list)
