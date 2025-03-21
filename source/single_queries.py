from db_from_csv import DBFromCsv
from connector import Connector
from tabulate import tabulate
import datetime

class QueryMaker:
    _connector:Connector

    def __init__(self):
        db = DBFromCsv()
        db.make_populated_table("Orders_combined", "orders_combined.csv")
        self._connector = Connector()

    #CREATE QUERIES
    # Add a new order to Orders_combined db
    def new_order(self, customer_name:str, customer_email:str,
                    product_name:str, product_price:float,
                    time:datetime.datetime|None = None):
        
        if not time:
            time = datetime.datetime.now()
        #Gets the number of sales. ID of sales starts at 0 <---IDs SHOULD start at 1
        #Will assume that it is the same as prev max id plus 1
        next_id = self._max_ID()+1
        
        query = f"""
                    INSERT INTO Orders_combined (
                        ID, date_time, `Customer_Name`, `Customer_email`,
                        `Product_Name`, `Product_Price`
                    )
                    VALUES (
                        {next_id}, '{time}','{customer_name}', '{customer_email}',
                        '{product_name}', {product_price}
                    )
                """
        self._connector.executeCUD(query)

    

    #READ QUERIES
    # Get full orders_combined table
    def select_all(self):
        query = "SELECT * FROM Orders_combined"
        return self._connector.executeR(query)
    
    # Pretty print full table
    def print_all(self):
        self.printR(self.select_all())
    
    # Spenders ordered by sum of money spent after given time
    def spenders_after(self, date:str):
        query = f"""SELECT CN AS Customer, SUM(PP) AS `Total Spending`
                    FROM (
                        SELECT `Customer_Name` AS CN, `Product_Price` AS PP
                        FROM Orders_combined
                        WHERE date_time > '{date}'
                        ) AS DER
                    GROUP BY Customer
                    ORDER BY `Total Spending` DESC"""
        return self._connector.executeR(query)
    
    # Products sorted by amount sold
    def nr_sales_by_product(self):
        query = f"""
                    SELECT `Product_Name` AS Name,
                        COUNT(`Product_Price`) as `Items sold`
                    FROM Orders_combined
                    GROUP BY Name
                    ORDER BY `Items sold` DESC
                """
        return self._connector.executeR(query)
    
    # Total number of sales
    def nr_sales(self):
        query = f"SELECT COUNT(ID) FROM Orders_combined"
        return self._connector.executeR(query)
    
    # Helper function to get the biggest id in table
    # Returns -1 if no items in table
    def _max_ID(self):
        query = "SELECT MAX(ID) FROM Orders_combined"
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
    #qm.printR(qm.nr_sales_by_product())
    
    #qm.new_order("Epsilon","not_my@email.com","yoga-mat",100)
    #qm.update_column_where_id("Customer_Name", 100, "Filippa")
    #qm.update_column_where_it_has_value("Product_Name", "200", "100")
    #qm.delete_rows_where_column_value("Customer_Name", "Wendy Lockman")
    #qm.delete_rows_from_before(datetime.datetime.now())
    #qm.print_all()
    qm.printR(qm.spenders_after(datetime.datetime(2025, 12, 1)))

if __name__ == "__main__":
    main()
