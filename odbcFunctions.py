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
try:
    cnxn = pyodbc.connect(connect_string)
    cursor = cnxn.cursor()
except Exception:
    print("Failed to connect")
    exit(0)
print("Connected...")

# Get sql server details
cursor.execute("SELECT @@version;") 
row = cursor.fetchone() 
if not row:
    print("No databases exist anywhere yet.")
while row: 
    print(row[0])
    row = cursor.fetchone()

# list tables
def listTables(connection):
    listTablesCommand = """ SELECT schema_name(t.schema_id) as schema_name, 
        t.name as table_name,
        t.create_date, 
        t.modify_date 
        from sys.tables t 
        order by schema_name, 
        table_name """
    tables = []
    print("\nlooking for tables again....")
    cursor = connection.cursor()
    cursor.execute(listTablesCommand)
    row = cursor.fetchone() 
    if not row:
        print("No tables exist anywhere yet.")
    while row: 
        print(f"{row}")
        tables += [row[1]]
        row = cursor.fetchone()
    cursor.close()
    return tables

# Create a table in the new database
def createTableIfNotExists(connection, tableName, fields):
    print(f"adding table {tableName}")
    cursor = connection.cursor()
    command = f""" if not exists (SELECT schema_name(t.schema_id) as schema_name,
        t.name as table_name,
        t.create_date,
        t.modify_date
        from sys.tables t
        where t.name = '{tableName}') """
    # command =  f"IF NOT EXISTS (select * from sys.tables where (name = {tableName})) "
    command += f"CREATE TABLE {tableName} (" + ', '.join(fields) + ")"
    # command += "PRINT('sponges')"
    cursor.execute(command)
    connection.commit()
    cursor.close()

# Drop a table 
def dropTableIfExists(connection, tableName):
    print(f"dropping table: {tableName}")
    cursor = connection.cursor()
    listTablesCommand2 = f""" SELECT schema_name(t.schema_id) as schema_name, 
        t.name as table_name,
        t.create_date, 
        t.modify_date 
        from sys.tables t 
        where t.name = '{tableName}'
        """
    command = f"if exists ({listTablesCommand2})"
    command += f"DROP TABLE {tableName}"
    cursor.execute(command)
    connection.commit()
    cursor.close()

# insert a record
def insertOne(connection, table, **fvPairs):
    print(f"inserting into table {table}")
    fields = ', '.join(list(fvPairs.keys()))
    values = tuple(list(fvPairs.values()))
    # print(f"fields: {fields}")
    # print(f"values: {values}")
    query = f"INSERT INTO {table} ({fields}) VALUES "+f'{values};'
    # print(query)
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
    except Exception as e:
        print(f"OOPS insert error: {e}")

# select all five records in the table
def selectWhere(connection, table, condition):
    command =  f"""SELECT * FROM {table} where {condition} """
    print(f"selectwhere command: {command}")
    cursor = cnxn.cursor()
    cursor.execute(command)
    row = cursor.fetchone()
    rows = []
    while row:
        print(row)
        row = cursor.fetchone()
        rows += [row]
    cursor.close()
    return rows


def main():
    listTables(cnxn)
    newTableNames = ['hf28','hf27','hf26','hf25','hf24']
    newTableFields = ["id integer identity(1,1)", "name varchar(32)", "value varchar(32)"]
    for t in newTableNames:
        createTableIfNotExists(cnxn, t, newTableFields)
        # insert N records in the table
        N = 5
        records = [{"name":f"{t}_mark", "value":str(i)} for i in range(N)]
        for r in records:
            insertOne(cnxn, t, **r)

    listTables(cnxn)

    for t in newTableNames:
        for v in range(N):
            r = selectWhere(cnxn, t, f"value = '{v}';")

    for t in newTableNames:
        dropTableIfExists(cnxn, t)


if __name__ == "__main__":
    main()