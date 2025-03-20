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
                        ID, date_time, `Customer Name`, `Customer email`,
                        `Product Name`, `Product Price`
                    )
                    VALUES (
                        {next_id}, '{time}','{customer_name}', '{customer_email}',
                        '{product_name}', {product_price}
                    )
                """
        self._connector.executeCUD(query)

    

    #READ QUERIES
    def select_all(self):
        query = "SELECT * FROM Orders_combined"
        return self._connector.executeR(query)
    
    def print_all(self):
        self.printR(self.select_all())
    
    def spenders_after(self, date:str):
        query = f"""SELECT CN AS Customer, SUM(PP) AS `Total Spending`
                    FROM (
                        SELECT `Customer Name` AS CN, `Product Price` AS PP
                        FROM Orders_combined
                        WHERE date_time > '{date}'
                        ) AS DER
                    GROUP BY Customer
                    ORDER BY `Total Spending` DESC"""
        return self._connector.executeR(query)
    
    def nr_sales_by_product(self):
        query = f"""
                    SELECT `Product Name` AS N, COUNT(`Product Price`) as Revenue
                    FROM Orders_combined
                    GROUP BY N
                    ORDER BY Revenue DESC
                """
        return self._connector.executeR(query)
    
    def nr_sales(self):
        query = f"SELECT COUNT(ID) FROM Orders_combined"
        return self._connector.executeR(query)
    
    def _max_ID(self):
        query = "SELECT MAX(ID) FROM Orders_combined"
        res, _ = self._connector.executeR(query)
        if res[0][0] is None:
            return -1
        return res[0][0]
    
    def printR(self, read_result):
        print(tabulate(read_result[0], read_result[1]))

    # UPDATE QUERIES
    def update_column_where_id(self, column_name, target_id, new_val):
        if type(new_val) == str and " " in new_val:
            new_val = f"'{new_val}'"
        query = f"""
                    UPDATE Orders_combined
                    SET `{column_name}` = '{new_val}'
                    WHERE ID = {target_id}
                """
        self._connector.executeCUD(query)
        
    def update_column_where_it_has_value(self, column_name, current_val, new_val):
        if type(current_val) == str and " " in current_val:
            current_val = f"'{current_val}'"
        if type(new_val) == str and " " in new_val:
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
    
    # DELETE QUERIES
    def delete_rows_where_column_value(self, column_name, current_val):
        if type(current_val) == str and " " in current_val:
            current_val = f"'{current_val}'"
        query = f"""
                    DELETE FROM Orders_combined
                    WHERE `{column_name}` = {current_val}
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Incorrect arguments for query!")
    
    def delete_row_where_id(self, target_id):
        self.delete_rows_where_column_value("ID", target_id)

    def delete_rows_from_before(self, timestamp:datetime.datetime):
        query = f"""
                    DELETE FROM Orders_combined
                    WHERE date_time < '{timestamp}'
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Incorrect arguments for query!")

    def delete_rows_from_after(self, timestamp:datetime.datetime) -> None:
        query = f"""
                    DELETE FROM Orders_combined
                    WHERE '{timestamp}' < date_time
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Incorrect arguments for query!")
    
def main():
    qm = QueryMaker()
    #qm.printR(qm.nr_sales_by_product())
    
    qm.new_order("Epsilon","not_my@email.com","yoga-mat",100)
    qm.update_column_where_id("Customer Name", 100, "Filippa")
    qm.update_column_where_it_has_value("Product Name", 200, 100)
    qm.print_all()
    


if __name__ == "__main__":
    main()
