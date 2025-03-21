from db_from_csv import DBFromCsv
from connector import Connector
from tabulate import tabulate
import datetime


class MultiQueryMaker:
    _connector: Connector

    # Assumes that Database orders is up and running and is populated
    # with tables matching those in ./data
    def __init__(self):
        self._connector = Connector(dbname="orders", exists=True)

    # CREATE QUERIES
    # Add a new order to Orders db. Assumes that product and customer
    # already exists
    def new_order(
        self, customer: int, product: int, time: datetime.datetime | None = None
    ):

        if not time:
            time = datetime.datetime.now()
        # Will assume that it is the same as prev max id plus 1
        try:
            next_id = self._max_ID("Orders") + 1
        except:
            print(f"Table Orders does not exist")
            return

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
            print(
                f"Either {customer=} or {product} does not exist in"
                + "their respective tables"
            )

    # Adds new product
    def new_product(self, name: str, price: float):
        try:
            next_id = self._max_ID("products") + 1
        except:
            print(f"Table Products does not exist")
            return

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
    def new_customer(self, name: str, email: str):
        try:
            next_id = self._max_ID("customers") + 1
        except:
            print(f"Table Customers does not exist")
            return

        query = f"""
                    INSERT INTO Customers (
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

    # READ QUERIES
    # Get table corresponding to orders combined
    def select_all(self, limit: int | None = None, offset: int | None = None):
        query = """SELECT Orders.id AS id, date_time,
                    Customers.name AS customer_name, email AS customer_email,
                    Products.name AS product_name, price AS product_price
                    FROM Orders
                    INNER JOIN Products ON product = Products.id
                    INNER JOIN Customers On customer = Customers.id
                    ORDER BY id ASC
                """
        if limit is not None:
            query += f" LIMIT {limit}"
        if offset is not None:
            query += f" OFFSET {offset}"
        return self._connector.executeR(query)
    
    # Get all from table
    def get_table(self, table:str):
        query = f"""
                    SELECT *
                    FROM `{table}`
                """
        return self._connector.executeR(query)

    # Pretty print full table
    def print_all(self, limit = None, offset = None):
        self.printR(self.select_all(limit, offset))

    # Spenders ordered by sum of money spent after given time
    # Defaults to right now
    def spenders_after(self, date: datetime.datetime | None = None):
        if date is None:
            date = datetime.datetime.now()
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
                    SELECT Products.name AS Name,
                        COUNT(Orders.id) as `Items sold`
                    FROM Orders
                    INNER JOIN Products ON product = Products.id
                    GROUP BY Name
                    ORDER BY `Items sold` DESC
                """
        return self._connector.executeR(query)

    # Total number of sales
    def nr_sales(self):
        query = f"SELECT COUNT(ID) AS `Items sold` FROM Orders"
        return self._connector.executeR(query)

    # Helper function to get the biggest id in table
    # Returns -1 if no items in table
    def _max_ID(self, table: str):
        query = f"SELECT MAX(ID) FROM {table}"
        res, _ = self._connector.executeR(query)
        if res[0][0] is None:
            return -1
        return res[0][0]

    # Pretty print for return of queries
    def printR(self, read_result):
        print(tabulate(read_result[0], read_result[1]))

    # UPDATE QUERIES
    # Update any column in orders combined based on any other
    def update_whole_schema(self,
                            reason_table, target_table,
                            reason_column, target_column,
                            reason_val, new_val):
        if not isinstance(reason_val, (int, float)):
            reason_val = f"'{reason_val}'"
        if not isinstance(new_val, (int, float)):
            new_val = f"'{new_val}'"
        query = f"""
                    UPDATE orders
                    INNER JOIN products ON orders.product = products.id
                    Inner join customers on orders.customer = customers.id
                    SET `{target_table}`.`{target_column}` = {new_val}
                    WHERE `{reason_table}`.`{reason_column}` = {reason_val};
                """
        self._connector.executeCUD(query)

    # Update item from column where ID matches in table
    def update_row_where_id(self, table, column_name, target_id, new_val):
        query = f"""
                    UPDATE {table}
                    SET `{column_name}` = '{new_val}'
                    WHERE ID = {target_id}
                """
        self._connector.executeCUD(query)

    # Update value in column in table where
    # the value of the column matches
    def update_column_where_it_has_value(
        self, table, column_name: str, current_val, new_val
    ):
        if not isinstance(current_val, (int, float)):
            current_val = f"'{current_val}'"
            new_val = f"'{new_val}'"
        query = f"""
                    UPDATE {table}
                    SET `{column_name}` = {new_val}
                    WHERE `{column_name}` = {current_val}
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Could not perform query!")
            if column_name.lower() == "id":
                print(f"Perhaps id {new_val} is already assigned")
            print(f"{query=}")

    # DELETE QUERIES
    # Delete every row where the value of given column matches
    def _delete_row(self, table, column_name, current_val):
        if not isinstance(current_val, (int, float)):
            current_val = f"'{current_val}'"
        query = f"""
                    DELETE FROM `{table}`
                    WHERE `{column_name}` = {current_val}
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Could not perform query!")
            print(f"{query=}")

    # Delete every order row where the value of given column matches
    def delete_orders_where_column_value(self, column_name, current_val):
        self._delete_row("Orders", column_name, current_val)

    # Delete order row where id matches
    def delete_order_where_id(self, target_id):
        self.delete_orders_where_column_value("ID", target_id)

    # Delete product and corresponding orders
    def delete_product_and_matching_orders(self, target_id):
        # Works to delete orders as well since we have ON DELETE CASCADE
        # added for the foreign key relation
        self._delete_row("Products", "ID", target_id)

    # Delete products and corresponding orders
    def delete_products_and_matching_orders(self, target_column, target_value):
        # Works to delete orders as well since we have ON DELETE CASCADE
        # added for the foreign key relation
        self._delete_row("Products", target_column, target_value)

    # Delete customer and corresponding orders
    def delete_customer_and_matching_orders(self, target_id):
        # Works to delete orders as well since we have ON DELETE CASCADE
        # added for the foreign key relation
        self._delete_row("Customers", "ID", target_id)
    
    # Delete customer and corresponding orders
    def delete_customers_and_matching_orders(self, target_column, target_value):
        # Works to delete orders as well since we have ON DELETE CASCADE
        # added for the foreign key relation
        self._delete_row("Customers", target_column, target_value)

    # Delete rows with date_time is older than given time
    def delete_rows_from_before(self, timestamp: datetime.datetime):
        query = f"""
                    DELETE FROM Orders
                    WHERE date_time < '{timestamp}'
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Incorrect arguments for query!")
            print(f"{query=}")

    # Delete rows with date_time is newer than given time
    def delete_rows_from_after(self, timestamp: datetime.datetime) -> None:
        query = f"""
                    DELETE FROM Orders
                    WHERE '{timestamp}' < date_time
                """
        try:
            self._connector.executeCUD(query)
        except:
            print("Incorrect arguments for query!")
            print(f"{query=}")


def main():
    db = DBFromCsv()
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
    qm = MultiQueryMaker()
    # qm.print_all()
    # qm.printR(qm.spenders_after(datetime.datetime(2025, 12, 1)))
    # qm.printR(qm.nr_sales_by_product())
    qm.update_column_where_it_has_value("products", "id", 1, 15)
    qm.print_all()


if __name__ == "__main__":
    main()
