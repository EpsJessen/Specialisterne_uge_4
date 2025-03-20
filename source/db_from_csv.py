from connector import Connector
from os.path import join
import csv
from datetime import datetime

# Class for creating a database from a csv file using an instance of the
# Connector class
class DBFromCsv:
    _connector: Connector

    def __init__(self):
        # Creates a fresh db using the standard
        # settings defined by Connector
        self._connector = Connector(exists=False)

    #Adds table to schema
    def add_table(self, table_name:str, headers,
                  row, fk_dicts, pk:str = "ID") -> None:
        fields = ""
        #Adds each field one by one to query
        for i, name in enumerate(headers):
            fields += f"`{name}` {self.item_to_sql_type(row[i])} NOT NULL,"
        #Adds primary key
        fields += f"PRIMARY KEY (`{pk}`)"
        if fk_dicts:
            for fk in fk_dicts:
                fields += f", FOREIGN KEY (`{fk["fk"]}`) REFERENCES `{fk["table"]}`(`{fk["key"]}`)"

        #Creates full query
        create_table_stmnt = f"CREATE TABLE {table_name}({fields});"

        # Drops table if it already exists in the schema, and then adds
        # current version
        self._connector.executeCUD(f"DROP TABLE IF EXISTS {table_name}")
        self._connector.executeCUD(create_table_stmnt)

    # Adds rows from csv file to table with corresponding headers
    def populate_table(self, table_name:str, headers, rows):
        sql_insert = "INSERT INTO %s ("
        # Adds each field to query
        for header in headers:
            sql_insert += f"`{header}`, "
        sql_insert = sql_insert[:-2]
        # DETERMINEs WHETHER EACH VALUE SHOULD
        # BE EXPLICITLY MARKED AS STRING
        sql_insert += f") VALUES ({self.stringy_substitude(rows[0])})"
        
        # Determine which rows contain datetimes
        datetime_columns = self.datetime_columns(rows[0])
        #Adds each row to the table
        for row in rows:
            row = [table_name] + row
            # Ensures that datetimes have proper format
            for i in datetime_columns:
                row[i+1]=datetime.fromisoformat(row[i+1])
            self._connector.executeCUD(sql_insert % tuple(row))
    
    # Make and populate a table with data from csv file
    def make_populated_table(self, table_name, file_name, fk_dicts):
        # Get data from csv file
        os_path = join("data", file_name)
        with open(os_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)
            rows = list(csv_reader)
        self.add_table(table_name, headers, rows[0], fk_dicts)
        self.populate_table(table_name, headers, rows)

    # Make and populate multiple tables
    def make_populated_tables(self, table_names, table_files, lists_fk_dicts):
        # Make each table sequentially
        for i, name in enumerate(table_names):
            self.make_populated_table(name, table_files[i], lists_fk_dicts[i])

    # Convert csv data to correct format if it is not meant to be string
    def try_convert(self, input):
        try:
            dt_obj = datetime.fromisoformat(input)
            return dt_obj
        except:
            pass
        try:
            return int(input)
        except:
            pass
        try:
            return float(input)
        except:
            return input
    
    # Determine which columns contain datetime data
    def datetime_columns(self, row):
        columns = []
        for i, item in enumerate(row):
            type_item = self.try_convert(item)
            if isinstance(type_item, datetime):
                columns.append(i)
        return columns
    
    # Determine if data should be sent to database explicitly as string
    def stringy_substitude(self, row):
        substitudes = ""
        for item in row:
            type_item = self.try_convert(item)
            if isinstance(type_item, (int, float)):
                substitudes += "%s, "
            else:
                substitudes += "'%s', "
        # remove last ,_ from string
        return substitudes[:-2]

    # Determine which sql type corresponds to data
    def item_to_sql_type(self, item) -> str:
        type_item = self.try_convert(item)
        if isinstance(type_item, datetime):
            return "DATETIME"
        elif isinstance(type_item, int):
            return "int"
        elif isinstance(type_item, float):
            return "float"
        else:
            return "VARCHAR(250)"

def main():
    db = DBFromCsv()
    #db.make_populated_table("Orders_combined", "orders_combined.csv")
    #os_path = join("data", "orders_combined.csv")
    #with open(os_path, "r") as csv_file:
    #    csv_reader = csv.reader(csv_file)
    #    _ = next(csv_reader)
    #    vals = next(csv_reader)
    #    for val in vals:
    #        print(f"{val} is of sql_type {db.item_to_sql_type(val)}")
    db.make_populated_tables(["customers", "products", "orders"],
                             ["customers.csv", "products.csv", "orders.csv"],
                             [None,None,[{"table":"customers", "key":"id",
                                      "fk":"customer"},
                                     {"table":"products", "key":"id",
                                      "fk":"product"}
                                      ]]
                            )

if __name__ == "__main__":
    main()
