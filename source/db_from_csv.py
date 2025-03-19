from connector import Connector
from os.path import join
import csv

# Class for creating a database from a csv file using an instance of the
# Connector class
class DBFromCsv:
    _connector: Connector

    def __init__(self):
        # Creates a fresh db using the standard
        # settings defined by Connector
        self._connector = Connector(exists=False)

    def add_table(self, table_name:str) -> None:
        fields = """ID int NOT NULL,
                    time DATETIME,
                    `Customer Name` VARCHAR(250) NOT NULL,
                    `Customer Email` VARCHAR(250) NOT NULL,
                    `Product Name` VARCHAR(100) NOT NULL,
                    `Product Price` float NOT NULL,
                    PRIMARY KEY (ID)
                    """

        create_table_stmnt = f"CREATE TABLE {table_name}({fields});"

        self._connector.executeCUD(f"DROP TABLE IF EXISTS {table_name}")
        self._connector.executeCUD(create_table_stmnt)

    def populate_table(self, table_name:str, csv_file:str):
        os_path = join("data", csv_file)

        sql_insert = """INSERT INTO %s (
                            ID, time, `Customer Name`, `Customer Email`,
                            `Product Name`, `Product Price`)
                            VALUES (%s, '%s', '%s', '%s', '%s', %s)"""
        
        with open(os_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            _ = next(csv_reader)

            for row in csv_reader:
                row = [table_name] + row
                row[2].replace("T", " ")
                self._connector.executeCUD(sql_insert % tuple(row))
        
    def make_populated_table(self, table_name, csv_file):
        self.add_table(table_name)
        self.populate_table(table_name, csv_file)

    

