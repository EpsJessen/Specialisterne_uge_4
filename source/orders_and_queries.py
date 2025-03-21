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

def pause() -> None:
    _ = input("Press enter to continue")

def main():
    populate_db()
    single = SingleQueryMaker()
    relational = MultiQueryMaker()
    #SHOW EACH QUERY, WAIT FOR INPUT TO PERFORM AND SHOW NEXT
    print("Read all orders combined (show first 10)")
    pause()
    relational.print_all(limit=10)
    pause()
    print("Read biggest spenders after right now")
    pause()
    relational.printR(relational.spenders_after())
    pause()
    print("Read products by amount sold")
    pause()
    relational.printR(relational.nr_sales_by_product())
    pause()
    print("Read total number of sales")
    pause()
    relational.printR(relational.nr_sales())
    pause()
    print("Create new product, Dock, and print all products after")
    relational.new_product("Dock", 119.95)
    pause()
    relational.printR(relational.get_table("Products"))
    print("Create new customer Epsilon, and print all customers after")
    relational.new_customer("Epsilon", "not@my.email")
    pause()
    relational.printR(relational.get_table("Customers"))
    pause()
    print("Create new order, then print last 10 orders combined")
    relational.new_order(30, 10)
    pause()
    relational.print_all(limit=10, offset=91)
    pause()
    print("Update order 99 to also be of Dock, then print last ten orders")
    relational.update_row_where_id("Orders", "product", 99, 10)
    pause()
    relational.print_all(limit=10, offset=91)
    pause()
    print("Change the name of everyone who bougth a mouse to Rat King,",
    " then print all customers")
    relational.update_whole_schema("Products", "Customers", "name", "name", "mouse", "Rat King")
    pause()
    relational.printR(relational.get_table("Customers"))
    print("Rodent extermination! Remove rat kings and all their orders",
          " as well as mouse")
    relational.delete_customers_and_matching_orders("name", "Rat King")
    relational.print_all()
    





if __name__ == "__main__":
    main()
