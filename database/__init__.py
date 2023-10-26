from clickhouse_driver import Client
from typing import *
import json


class DatabaseManager:
    def __init__(self, database: str, tables: dict, backup_folder: str) -> None:
        self.database = database
        self.tables = tables
        self.backup_folder = backup_folder

        self.client = Client("localhost")

        self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        for table in self.tables.values():
            self.client.execute(
                f"CREATE TABLE IF NOT EXISTS {self.database}.{table['name']} ({table['values']}) ENGINE = Memory"
            )

    def execute(self, command: str, *args, **kwargs):
        self.client.execute(command, *args, **kwargs)

    def insert(self, table: str, values: str) -> None:
        self.client.execute(
            f"INSERT INTO {self.database}.{self.tables[table]['name']} ({self.tables[table]['insert']}) VALUES ({values})"
        )

    def select(self, table: str, equal: str = "1=1") -> any:
        return self.client.execute(f"SELECT * FROM {self.database}.{self.tables[table]['name']} WHERE {equal}")

    def delete(self, table: str, equal: str) -> None:
        self.client.execute(
            f"ALTER TABLE {self.database}.{self.tables[table]['name']} DELETE WHERE {equal}"
        )

    def clear_table(self, table: str) -> None:
        self.client.execute(
            f"ALTER TABLE {self.database}.{self.tables[table]['name']} DELETE WHERE 1=1"
        )

    def save_backup(self, backup_name: str) -> None:
        data = {}
        for table in self.tables.items():
            _data = []
            for item in self.select(table[0]):
                _data.append(f"{item}")
            data[table[1]['name']] = _data
        json.dump(
            data,
            open(f"{self.backup_folder}/{backup_name}.json", "w+"),
            indent=4
        )
        json.dump(
            data,
            open(f"{self.backup_folder}/last_backup.json", "w+"),
            indent=4
        )

    def load_backup(self, backup_name: str) -> None:
        try:
            for table, values in json.load(open(f"{self.backup_folder}/{backup_name}.json", "r")).items():
                if len(values) == 0:
                    continue
                for value in values:
                    self.insert(table.lower(), value.replace(
                        "(", "").replace(")", ""))
        except:
            return