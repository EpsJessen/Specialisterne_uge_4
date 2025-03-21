# Specialisterne_uge_4

 Code for interacting with MySQL database, for week 4 of specialisterne academy

### To-Dos:

- [x] Create Connector, class for handling connections to MySQL
- [x] Create DB_From_Csv, class for populating database using Connector
  - [x] Refactor to handle multiple tables with FK relations
- [x] Create CRUD, class for making **relevant** CRUD calls using Connector
  - [x] Analyse database for relevant calls
  - [x] Refactor to handle tables with FK

### Running program:

To run the code, first install the requirements in the venv.  
Then change `credentials.json` to match yours for MySQL.  
Then run the file `orders_and_queries.py` to create the database and 
see common queries and their results when used on the database.
Will alternately print what will be shown next, and the described data.
To go to the next element, press enter.
