from db_from_csv import DBFromCsv
from connector import Connector
from tabulate import tabulate
import datetime

class QueryMaker:
    _connector:Connector

    def __init__(self):
        db = DBFromCsv()
        db.make_populated_table("Orders_combined", "orders_combined.csv")
        db.make_populated_tables(["customers", "products", "orders"],
                             ["customers.csv", "products.csv", "orders.csv"],
                             [None,None,[{"table":"customers", "key":"id",
                                      "fk":"customer"},
                                     {"table":"products", "key":"id",
                                      "fk":"product"}
                                      ]]
                            )
        self._connector = Connector()

    #CREATE QUERIES
    # Add a new order to Orders db. Assumes that product and customer
    # already exists
    def new_order(self, customer: int,
                    product: int,
                    time:datetime.datetime|None = None):
        
        if not time:
            time = datetime.datetime.now()
        #Will assume that it is the same as prev max id plus 1
        next_id = self._max_ID("Orders")+1
        
        query = f"""
                    INSERT INTO Orders (
                        ID, date_time, `Customer`, `Product`
                    )
                    VALUES (
                        {next_id}, '{time}','{customer}', {product}
                    )
                """
        try:
            self._connector.executeCUD(query)
        except:
            print(f"Either {customer=} or {product} does not exist in"
                  +"their respective tables")

    # Adds new product
    def new_product(self, name:str, price:float):
        next_id = self._max_ID("products")+1
        
        query = f"""
                    INSERT INTO Products (
                        ID, name, price
                    )
                    VALUES (
                        {next_id}, '{name}','{price}'
                    )
                """
        try:
            self._connector.executeCUD(query)
        except:
            print(f"Could not perform {query=}")

    # Adds new customer
    def new_product(self, name:str, email:str):
        next_id = self._max_ID("customers")+1
        
        query = f"""
                    INSERT INTO Products (
                        ID, name, email
                    )
                    VALUES (
                        {next_id}, '{name}','{email}'
                    )
                """
        try:
            self._connector.executeCUD(query)
        except:
            print(f"Could not perform {query=}")

    #READ QUERIES
    # Get table corresponding to orders combined
    def select_all(self):
        query = """SELECT Orders.id AS id, date_time,
                    Customers.name AS customer_name, email AS customer_email,
                    Products.name AS product_name, price AS product_price
                    FROM Orders
                    INNER JOIN Products ON product = Products.id
                    INNER JOIN Customers On customer = Customers.id
                    ORDER BY id ASC
                """
        return self._connector.executeR(query)
    
    # Pretty print full table
    def print_all(self):
        self.printR(self.select_all())
    
    # Spenders ordered by sum of money spent after given time
    def spenders_after(self, date:str):
        query = f"""SELECT name AS Customer, TS AS `Total Spending`
                    FROM (
                        SELECT `customer` AS CID, SUM(`Price`) AS TS
                        FROM Orders
                        INNER JOIN Products ON product = Products.id
                        WHERE date_time > '{date}'
                        GROUP BY Customer
                        ) AS DER
                    INNER JOIN Customers ON CID = Customers.id
                    ORDER BY `Total Spending` DESC"""
        return self._connector.executeR(query)
    
    # Products sorted by amount sold
    def nr_sales_by_product(self):
        query = f"""
                    SELECT `Product_Name` AS N, COUNT(`Product_Price`) as Revenue
                    FROM Orders_combined
                    GROUP BY N
                    ORDER BY Revenue DESC
                """
        return self._connector.executeR(query)
    
    # Total number of sales
    def nr_sales(self):
        query = f"SELECT COUNT(ID) FROM Orders_combined"
        return self._connector.executeR(query)
    
    # Helper function to get the biggest id in table
    # Returns -1 if no items in table
    def _max_ID(self, table:str):
        query = f"SELECT MAX(ID) FROM {table}"
        res, _ = self._connector.executeR(query)
        if res[0][0] is None:
            return -1
        return res[0][0]
    
    # Pretty print for retur of queries
    def printR(self, read_result):
        print(tabulate(read_result[0], read_result[1]))

    # UPDATE QUERIES
    # Update value in column where ID matches
    def update_column_where_id(self, column_name, target_id, new_val):
        query = f"""
                    UPDATE Orders_combined
                    SET `{column_name}` = '{new_val}'
                    WHERE ID = {target_id}
                """
        self._connector.executeCUD(query)
    
    # Update value in column where the value of the column matches
    def update_column_where_it_has_value(self, column_name,
                                         current_val, new_val):
        if not isinstance(current_val, (int, float)):
            current_val = f"'{current_val}'"
            new_val = f"'{new_val}'"
        query = f"""
                    UPDATE Orders_combined
                    SET `{column_name}` = {new_val}
                    WHERE `{column_name}` = {current_val}
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Incorrect arguments for query!")
            print(f"{query=}")
    
    # DELETE QUERIES
    # Delete every row where the value of given column matches 
    def delete_rows_where_column_value(self, column_name, current_val):
        if not isinstance(current_val, (int, float)):
            current_val = f"'{current_val}'"
        query = f"""
                    DELETE FROM Orders_combined
                    WHERE `{column_name}` = {current_val}
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Incorrect arguments for query!")
            print(f"{query=}")

    # Delete row where id matches
    def delete_row_where_id(self, target_id):
        self.delete_rows_where_column_value("ID", target_id)

    # Delete rows with date_time is older than given time
    def delete_rows_from_before(self, timestamp:datetime.datetime):
        query = f"""
                    DELETE FROM Orders_combined
                    WHERE date_time < '{timestamp}'
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Incorrect arguments for query!")
            print(f"{query=}")

    # Delete rows with date_time is newer than given time
    def delete_rows_from_after(self, timestamp:datetime.datetime) -> None:
        query = f"""
                    DELETE FROM Orders_combined
                    WHERE '{timestamp}' < date_time
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Incorrect arguments for query!")
            print(f"{query=}")
    
def main():
    qm = QueryMaker()
    #qm.print_all()
    qm.printR(qm.spenders_after(datetime.datetime(2025, 12, 1)))


if __name__ == "__main__":
    main()
