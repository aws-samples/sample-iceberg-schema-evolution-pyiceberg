from enum import Enum
from typing import Any

from pyiceberg.types import StringType, StructType, LongType, FloatType, DoubleType, DateType, TimeType, UUIDType, \
    BinaryType, FixedType, DecimalType, BooleanType, IntegerType, IcebergType, TimestampType


class IcebergDataType(Enum):
    """
    Enum to enforce proper types and lookups before trying to create iceberg table.
    Addresses incorrect or unhandled column type errors. The type_set property is
    an array of iceberg -> chime type mappings. Allows for multiple chime type mappings
     for e.g. source type = int|integer will map to pyiceberg.types.IntegerType
    """

    FIXED_TYPE = (
        "fixed_type",
        "FixedType",
        {"fixed"},
        FixedType(4),
        False,
    )
    DECIMAL_TYPE = (
        "decimal_type",
        "DecimalType",
        {"decimal"},
        DecimalType(5, 5),
        False,
    )
    LIST_TYPE = ("list_type", "ListType", {"list", "array"}, StringType(), True)
    STRUCT_TYPE = ("struct_type", "StructType", {"struct"}, StructType(), True, True)
    ARRAY_TYPE = (
        "array_type",
        "ListType",
        {"list"},
        StringType(),
        True,
    )
    MAP_TYPE = (
        "map_type",
        "MapType",
        {"map"},
        StringType(),
        True,
    )  # Just use StringType as native type.
    BOOLEAN_TYPE = (
        "boolean_type",
        "BooleanType",
        {"boolean", "bool"},
        BooleanType(),
        False,
    )
    INTEGER_TYPE = (
        "integer_type",
        "IntegerType",
        {"integer", "int"},
        IntegerType(),
        False,
    )
    LONG_TYPE = ("long_type", "LongType", {"bigint", "long"}, LongType(), False)
    FLOAT_TYPE = ("float_type", "FloatType", {"float"}, FloatType(), False)
    DOUBLE_TYPE = ("double_type", "DoubleType", {"double"}, DoubleType(), False)
    DATE_TYPE = ("date_type", "DateType", {"date"}, DateType(), False)
    TIME_TYPE = ("time_type", "TimeType", {"time"}, TimeType(), False)
    TIMESTAMP_TYPE = (
        "timestamp_type",
        "TimestampType",
        {"timestamp"},
        TimestampType(),
        False,
    )
    TIMEZONE_TYPE = (
        "timezone_type",
        "TimezoneType",
        {"timezone"},
        TimestampType(),
        False,
    )
    STRING_TYPE = ("string_type", "StringType", {"string", "str"}, StringType(), False)
    UUID_TYPE = ("uuid_type", "UUIDType", {"uuid"}, UUIDType(), False)
    BINARY_TYPE = ("binary_type", "BinaryType", {"binary"}, BinaryType(), False)
    UNKNOWN_TYPE = ("unknown_type", "UnknownType", {"unknown"}, StringType(), False)

    def __init__(
        self,
        value: str,
        description: str,
        type_set: set[str],
        native_iceberg_type: IcebergType,
        is_nested_type: bool,
        can_flatten: bool = False,
    ):
        self._value = value
        self._description = description
        self._is_nested_type = is_nested_type
        self._type_set = type_set
        self._native_iceberg_type = native_iceberg_type
        self._can_flatten = can_flatten

    @property
    def is_nested_type(self):
        return self._is_nested_type

    @property
    def value(self):
        return self._value

    @property
    def description(self):
        return self._description

    @property
    def type_set(self):
        return self._type_set

    @property
    def native_iceberg_type(self):
        return self._native_iceberg_type

    @property
    def can_flatten(self):
        return self._can_flatten



def find_enum_by_attribute(enum_class, attribute_name, attribute_value) -> Any | None:
    for member in enum_class:
        if (
            hasattr(member, attribute_name)
            and getattr(member, attribute_name).lower() == attribute_value.lower()
        ):
            return member
    return None


def map_config_type_to_iceberg_type(type_value: str) -> IcebergDataType:
    type_lower = type_value.casefold()
    return_type: IcebergDataType = IcebergDataType.UNKNOWN_TYPE
    if type_value.lower().startswith("array"):
        return_type = IcebergDataType.LIST_TYPE
    elif type_value.lower().startswith("struct"):
        return_type = IcebergDataType.STRUCT_TYPE
    elif type_value.lower().startswith("map"):
        return_type = IcebergDataType.MAP_TYPE
    else:
        for member in IcebergDataType:
            if member.type_set.__contains__(type_lower):
                return_type = member
                break
    return return_type
