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
        #Thus the next ID will be the same as the number of previous orders
        next_id = self.nr_sales()[0][0][0]
        
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
    
    def printR(self, read_result):
        print(tabulate(read_result[0], read_result[1]))

    # UPDATE QUERIES
    # DELETE QUERIES
    
def main():
    qm = QueryMaker()
    #qm.printR(qm.nr_sales_by_product())
    
    qm.new_order("Epsilon","not_my@email.com","yoga-mat",100)
    qm.print_all()
    


if __name__ == "__main__":
    main()
