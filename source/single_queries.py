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

    
def main():
    qm = QueryMaker()
    qm.printR(qm.spenders_after("2025-10-01"))
    qm.print_all()
    


if __name__ == "__main__":
    main()
