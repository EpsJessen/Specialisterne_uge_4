import mysql.connector
import mysql.connector.cursor
from os.path import join
import json


# Class for handling interactions with a MySQL server and database
class Connector:
    _connection: (
        mysql.connector.pooling.PooledMySQLConnection
        | mysql.connector.MySQLConnection
        | None
    )
    host: str
    user: str
    _passwd: str
    dbname: str

    def __init__(self, dbname: str = "orders", exists: bool = True):
        credentials_file = join("credentials.json")
        with open(credentials_file) as json_credentials:
            credentials: dict = json.load(json_credentials)
        self.host = credentials.get("host")
        self.user = credentials.get("user")
        self._passwd = credentials.get("passwd")
        self.dbname = dbname
        self._connection = None
        self._set_connection(exists)
        if not exists:
            self._createdb()

    # Creates and sets the connection
    # Will try to connect to an existing database if it exists
    # as indicated in call
    def _set_connection(self, exists: bool = True):

        try:
            if self._connection and self._connection.is_connected():
                self._connection.close()

            if exists:
                self._connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    passwd=self._passwd,
                    database=self.dbname,
                )

            else:
                self._connection = mysql.connector.connect(
                    host=self.host, user=self.user, passwd=self._passwd
                )
        except mysql.connector.errors.ProgrammingError as Error:
            print("Credentials not recognized!")
            raise Error

        except mysql.connector.errors.DatabaseError as Error:
            print("Could not find Database!")
            raise Error

    # Returns a cursorobject for the connection
    def _get_cursor(self):
        if self._connection:
            return self._connection.cursor()

    # Ensures that the connection points to the database
    # By dropping potentially existing schema, adding a blank
    # And setting the connection to point to that schema
    def _createdb(self):
        try:
            self.executeCUD(f"DROP SCHEMA IF EXISTS {self.dbname}")
            self.executeCUD(f"CREATE SCHEMA {self.dbname}")

            self._connection.close()
            self._set_connection()

        except:
            print("Could not connect to MySQL!")

    # Executes a single SQL query of a type modifying the database (CREATE
    # UPDATE or DELETE)
    def executeCUD(self, sql: str) -> None:
        cursor = self._get_cursor()
        cursor.execute(sql)
        cursor.close()
        self._connection.commit()

    # Executes a single SQL query of a type assaying the database (READ)
    def executeR(self, sql: str):
        cursor = self._get_cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        columns = cursor.column_names
        cursor.close()
        return result, columns


def main():
    orders_connector = Connector(exists=False)


if __name__ == "__main__":
    main()
