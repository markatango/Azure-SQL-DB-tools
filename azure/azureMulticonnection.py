import pyodbc 
import os
import time

def listRows(cursor):
    row = cursor.fetchone()
    while row:
        print(row)
        row = cursor.fetchone()



server = os.environ["AZURE_SQL_SERVER"]
username = os.environ["AZURE_SQL_USER"]
password = os.environ["AZURE_SQL_PW"]
driver = '{ODBC Driver 13 for SQL Server}'
database = "amya"
connect_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};"
connections = []
for i in range(10):

    try:
        cnxn = pyodbc.connect(connect_string)
        print(f"Connected...{i}")
        cursor = cnxn.cursor()
        connections += [cnxn]
        cursor.execute("SELECT @@version;") 
        row = cursor.fetchone() 
        if not row:
            print("No databases exist anywhere yet.")
        while row: 
            print(row[0])
            row = cursor.fetchone()
    except Exception:
        print(f"Failed to connect...{i}")
        # exit(0)
        
while connections:
    cnxn = connections.pop()
    cnxn.close()
    print("closed a connection")

