import mysql.connector

#CREATE SCHEMA
#NOTICE THAT THERE IS NO SCHEMA SELECTED IN CONNECTION REQUEST
dataBase = mysql.connector.connect(host="localhost", user="root", passwd="250707")

print(dataBase)

cursor = dataBase.cursor()

cursor.execute("DROP DATABASE IF EXISTS gfg")

cursor.execute("CREATE DATABASE gfg")

dataBase.close()

#CREATE TABLE
gfg = mysql.connector.connect(host="localhost", user="root", passwd="250707", database="gfg")

cursor = gfg.cursor()

cursor.execute("DROP TABLE IF EXISTS STUDENT")
student_record = """CREATE TABLE STUDENT (
                NAME VARCHAR(20) NOT NULL,
                BRANCH VARCHAR(50),
                ROLL INT NOT NULL,
                SECTION VARCHAR(5),
                AGE INT
                )
                """

cursor.execute(student_record)
gfg.close()

#INSERT INTO TABLE
gfg = mysql.connector.connect(host="localhost", user="root", passwd="250707", database="gfg")
cursor = gfg.cursor()

sql_insert = """INSERT INTO STUDENT (NAME, BRANCH, ROLL, SECTION, AGE)
VALUES (%s, %s, %s, %s, %s)"""
val = ("Ram", "CSE", "85", "B", "19")

cursor.execute(sql_insert, val)
gfg.commit()

vals = [("Nikhil", "CSE", "98", "A", "18"),
       ("Nisha", "CSE", "99", "A", "18"),
       ("Rohan", "MAE", "43", "B", "20"),
       ("Amit", "ECE", "24", "A", "21"),
       ("Anil", "MAE", "45", "B", "20"), 
       ("Megha", "ECE", "55", "A", "22"), 
       ("Sita", "CSE", "95", "A", "19")]

cursor.executemany(sql_insert, vals)
gfg.commit()

gfg.close()

#READ DATA FROM TABLE
gfg = mysql.connector.connect(host="localhost", user="root", passwd="250707", database="gfg")

cursor = gfg.cursor()

sql_get = "SELECT NAME, ROLL FROM STUDENT LIMIT 2 OFFSET 7"
cursor.execute(sql_get)

get_result = cursor.fetchall()

for x in get_result:
    print(x)

gfg.close()

#UPDATE DATA IN TABLE
#CREATE TABLE
gfg = mysql.connector.connect(host="localhost", user="root", passwd="250707", database="gfg")
cursor = gfg.cursor()

sql_update = "UPDATE STUDENT SET AGE = 23 WHERE NAME = 'Ram'"
cursor.execute(sql_update)
gfg.commit()

gfg.close()

#DELETE DATA FROM TABLE
gfg = mysql.connector.connect(host="localhost", user="root", passwd="250707", database="gfg")
cursor = gfg.cursor()

sql_delete = "DELETE FROM STUDENT WHERE NAME = 'Sita'"
cursor.execute(sql_delete)
gfg.commit()

gfg.close()

#DROP TABLE & DATABASE
gfg = mysql.connector.connect(host="localhost", user="root", passwd="250707", database="gfg")
cursor = gfg.cursor()

sql_drop = "DROP TABLE IF EXISTS STUDENT"
cursor.execute(sql_drop)
gfg.commit()

gfg.close()