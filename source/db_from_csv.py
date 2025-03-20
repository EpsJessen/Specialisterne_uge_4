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

    def add_table(self, table_name:str, headers, row, pk:str = "ID") -> None:
        fields = ""
        for i, name in enumerate(headers):
            fields += f"`{name}` {self.item_to_sql_type(row[i])} NOT NULL,"
        fields += f"PRIMARY KEY (`{pk}`)"

        create_table_stmnt = f"CREATE TABLE {table_name}({fields});"

        self._connector.executeCUD(f"DROP TABLE IF EXISTS {table_name}")
        self._connector.executeCUD(create_table_stmnt)

    def populate_table(self, table_name:str, headers, rows):
        sql_insert = "INSERT INTO %s ("
        
        for header in headers:
            sql_insert += f"`{header}`, "
        sql_insert = sql_insert[:-2]
        #DETERMINE WHETHER EACH VALUE SHOULD BE EXPLICITLY MARKED AS STRING
        sql_insert += f") VALUES ({self.stringy_substitude(rows[0])})"
        
        datetime_columns = self.datetime_columns(rows[0])
        for row in rows:
            #CONVERT DATETIMES TO PROPER FORMAT
            row = [table_name] + row
            for i in datetime_columns:
                row[i+1]=datetime.fromisoformat(row[i+1])
            self._connector.executeCUD(sql_insert % tuple(row))
        
    def make_populated_table(self, table_name, file_name):
        os_path = join("data", file_name)
        with open(os_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)
            rows = list(csv_reader)
        self.add_table(table_name, headers, rows[0])
        self.populate_table(table_name, headers, rows)

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
        
    def datetime_columns(self, row):
        columns = []
        for i, item in enumerate(row):
            type_item = self.try_convert(item)
            if isinstance(type_item, datetime):
                columns.append(i)
        return columns
    
    def stringy_substitude(self, row):
        substitudes = ""
        for item in row:
            type_item = self.try_convert(item)
            if isinstance(type_item, (int, float)):
                substitudes += "%s, "
            else:
                substitudes += "'%s', "
        return substitudes[:-2]

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
    db.make_populated_table("Orders_combined", "orders_combined.csv")
    os_path = join("data", "orders_combined.csv")
    with open(os_path, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        _ = next(csv_reader)
        vals = next(csv_reader)
        for val in vals:
            print(f"{val} is of sql_type {db.item_to_sql_type(val)}")

if __name__ == "__main__":
    main()
