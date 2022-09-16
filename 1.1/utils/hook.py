
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import csv


class ReadCSVHook(BaseHook):

    def __init__(self):
        super().__init__()

    def get_data(self, path:str, query_name:str):
        row_list = []
        with open(path, 'r', encoding='utf-8', newline='') as csvfile:
            readers = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for index, row in enumerate(readers):
                if index > 0:
                    r_row = (', '.join(row))
                    string = str(r_row.split(';')[1:])[1:-1]
                    row_list.append(f"({string})")

        return row_list, query_name
