from dataclasses import dataclass
from typing import Optional


@dataclass
class ProcessSchemaResponse:
    database_name:str
    table_name:str
    change_type: str
    message_list: list[str]
    output_location: Optional[str]
    has_error: bool = False

    def __init__(self):
        self.message_list = []
        self.has_error = False
        self.change_type = "TBD"
