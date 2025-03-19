# Specialisterne_uge_4

 Code for interacting with MySQL database, for week 4 of specialisterne academy

### To-Dos:

- [x] Create Connector, class for handling connections to MySQL
- [x] Create DB_From_Csv, class for populating database using Connector
  - [ ] Refactor to handle multiple tables with FK relations
- [ ] Create CRUD, class for making **relevant** CRUD calls using Connector
  - [ ] Analyse database for relevant calls
  - [ ] Refactor to handle tables with FK

### Running program:

To run the code, first install the requirements in the venv.  
Then change `credentials.txt` to match yours for MySQL.  
Then run the file `orders_and_queries.py` to create the database and 
see common queries and their results when used on the database
