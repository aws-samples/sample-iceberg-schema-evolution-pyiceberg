import datetime
import random

import pyarrow as pa
from pyiceberg.table import Table

from iceberg_helper import IcebergHelper


class GenerateData:
    def __init__(self):
        self.system_random = random.SystemRandom()

    def _get_random_order_items_v1(self, item_count: int) -> list:
        return_list: list = []

        for i in range(item_count):
            return_list.append({'item_id': f"item_{str(i)}", "price": round(self.system_random.uniform(10.0, 20.0), 2)})
            i = i + 1
        return return_list

    def _get_random_order_items_v2(self, item_count: int) -> list:
        return_list: list = []

        for i in range(item_count):
            return_list.append({'item_id': f"item_{str(i)}", "item_count": self.system_random.randint(1, 5),
                                "price": round(self.system_random.uniform(10.0, 20.0), 2)})
            i = i + 1
        return return_list

    def _get_address_v2(self) -> dict:
        return {
            "address_line": f"address_line_{self.system_random.randint(1, 100)}",
            "city": f"city_{self.system_random.randint(1, 100)}",
            "state": f"state_{self.system_random.randint(1, 100)}",
            "zip": f"zip_{self.system_random.randint(1, 100)}",
        }

    def _get_address_v1(self) -> dict:
        return {
            "city": f"city_{self.system_random.randint(1, 100)}",
            "state": f"state_{self.system_random.randint(1, 100)}",
        }

    def _get_random_date(self):
        year = self.system_random.randint(2020, 2025)
        month = self.system_random.randint(1, 12)
        day = self.system_random.randint(1, 28)
        return datetime.datetime(year, month, day, 1, 1, 1)

    def _get_random_order_v1(self) -> dict:
        return {
            "order_time": self._get_random_date(),
            "customer_name": f"Customer_{self.system_random.randint(1, 100)}",
            "address": self._get_address_v1(),
            "order_items": self._get_random_order_items_v1(self.system_random.randint(1, 50))
        }

    def _get_random_order_v2(self) -> dict:
        return {
            "order_time": self._get_random_date(),
            "customer_name": f"Customer_{self.system_random.randint(1, 100)}",
            "address": self._get_address_v2(),
            "order_items": self._get_random_order_items_v2(self.system_random.randint(1, 50))
        }

    def insert_order(self, database_name: str, table_name: str, version: str):

        ih: IcebergHelper = IcebergHelper()
        iceberg_table: Table = ih.load_table(database_name, table_name)
        order_count = self.system_random.randint(1, 20)
        table_data: list = []
        for o in range(order_count):
            if version == "v1":
                table_data.append(self._get_random_order_v1())
            elif version == "v2":
                table_data.append(self._get_random_order_v2())
        data_table = pa.Table.from_pylist(table_data, schema=iceberg_table.schema().as_arrow(), )
        iceberg_table.append(data_table)
