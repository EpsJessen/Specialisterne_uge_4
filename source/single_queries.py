from db_from_csv import DBFromCsv
from connector import Connector
import datetime
from tabulate import tabulate

class QueryMaker:
    _connector:Connector

    def __init__(self):
        db = DBFromCsv()
        db.make_populated_table("Orders_combined", "orders_combined.csv")
        self._connector = Connector()

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
    
    def nr_sales(self):
        query = f"""
                    SELECT `Product Name` AS N, COUNT(`Product Price`) as Revenue
                    FROM Orders_combined
                    GROUP BY N
                    ORDER BY Revenue DESC
                """
        return self._connector.executeR(query)
    
    def printR(self, read_result):
        print(tabulate(read_result[0], read_result[1]))
    
def main():
    qm = QueryMaker()
    qm.printR(qm.nr_sales())
    #qm.print_all()
    


if __name__ == "__main__":
    main()
