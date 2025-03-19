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

