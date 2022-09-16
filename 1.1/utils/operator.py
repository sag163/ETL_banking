import os

from typing import Any


from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from utils.hook import ReadCSVHook

class ReadCSVOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            query_name: str,
            filename: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.query_name = query_name
        self.filename = filename

    def execute(self, context: Any):
        folder_name = '/home/sag163/airflow/dags/utils/csv'
        files = os.listdir(folder_name)
        for item in files:
            print('item', item)
            if self.filename in item:
                path = f"{folder_name}/{item}"
                break

        print()
        reader = ReadCSVHook()
        return reader.get_data(path, self.query_name)

# folder_name = 'utils/csv'
# folder_path = os.path.abspath(folder_name)
# files = os.listdir(folder_path)

# print(files)