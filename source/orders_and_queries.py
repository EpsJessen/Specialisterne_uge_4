from db_from_csv import DBFromCsv
from single_queries import SingleQueryMaker
from multi_queries import MultiQueryMaker


def populate_db() -> None:
    db = DBFromCsv()
    db.make_populated_table("Orders_combined", "orders_combined.csv")
    db.make_populated_tables(
        ["customers", "products", "orders"],
        ["customers.csv", "products.csv", "orders.csv"],
        [
            None,
            None,
            [
                {"table": "customers", "key": "id", "fk": "customer"},
                {"table": "products", "key": "id", "fk": "product"},
            ],
        ],
    )


def main():
    populate_db()
    single = SingleQueryMaker()
    relational = MultiQueryMaker()


if __name__ == "__main__":
    main()
