import pyodbc 
import os
import time

class AzureSQLDB(object):
    def __init__(self):
        self.cnxn = None

    def listRows(self, cursor):
        row = cursor.fetchone()
        while row:
            print(row)
            row = cursor.fetchone()

    def connect(self):
        server = os.environ["AZURE_SQL_SERVER"]
        username = os.environ["AZURE_SQL_USER"]
        password = os.environ["AZURE_SQL_PW"]
        driver = '{ODBC Driver 13 for SQL Server}'
        database = "amya"
        connect_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};"
        try:
            self.cnxn = pyodbc.connect(connect_string)
        except Exception:
            print("Failed to connect")
            exit(0)
        print("Connected...")

    # Get sql server details
    def getServerDetails(self):
        cursor = self.cnxn.cursor()
        cursor.execute("SELECT @@version;") 
        row = cursor.fetchone() 
        if not row:
            print("No databases exist anywhere yet.")
        while row: 
            print(row[0])
            row = cursor.fetchone()

    # list tables
    def listTables(self):
        listTablesCommand = """ SELECT schema_name(t.schema_id) as schema_name, 
            t.name as table_name,
            t.create_date, 
            t.modify_date 
            from sys.tables t 
            order by schema_name, 
            table_name """
        tables = []
        print("\nlooking for tables again....")
        cursor = self.cnxn.cursor()
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
    def createTableIfNotExists(self, tableName, fields):
        print(f"adding table {tableName}")
        cursor = self.cnxn.cursor()
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
        self.cnxn.commit()
        cursor.close()

    # Drop a table 
    def dropTableIfExists(self, tableName):
        print(f"dropping table: {tableName}")
        cursor = self.cnxn.cursor()
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
        self.cnxn.commit()
        cursor.close()

    # insert a record
    def insertOne(self,table, **fvPairs):
        print(f"inserting into table {table}")
        fields = ', '.join(list(fvPairs.keys()))
        values = tuple(list(fvPairs.values()))
        query = f"INSERT INTO {table} ({fields}) VALUES "+f'{values};'
        try:
            cursor = self.cnxn.cursor()
            cursor.execute(query)
            self.cnxn.commit()
            cursor.close()
        except Exception as e:
            print(f"OOPS insert error: {e}")

    # insert many records
    def insertMany(self, table, fvList):
        print(f"inserting multiple records into table {table}")
        cursor = self.cnxn.cursor()
        for fv in fvList:
            fields = ', '.join(list(fv.keys()))
            values = tuple(list(fv.values()))
            query = f"INSERT INTO {table} ({fields}) VALUES "+f'{values};'
            try:
                cursor.execute(query)
            except Exception as e:
                print(f"OOPS insert error: {e}")
        self.cnxn.commit()
        cursor.close()

    # insert many records
    # records = [{"name":f"{t}_mark", "value":str(i)} for i in range(N)]
    def insertMany2(self, table, fvList):
        print(f"inserting multiple records into table {table}")
        cursor = self.cnxn.cursor()

        fields = ", ".join(list(fvList[0].keys())) # all the keys must be the same
        values = [tuple(list(fv.values())) for fv in fvList]

        # conglomerate the values into the query string
        queryValues = [str(v) for v in values]
        query = f"INSERT INTO {table} ({fields}) VALUES " +','.join(queryValues)
        print(query)
        try:
            cursor.execute(query)
        except Exception as e:
            print(f"OOPS insert error: {e}")
        self.cnxn.commit()
        cursor.close()

    # select all five records in the table
    def selectWhere(self, table, condition):
        command =  f"""SELECT * FROM {table} where {condition} """
        cursor = self.cnxn.cursor()
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
    asdb = AzureSQLDB()
    asdb.connect()
    asdb.getServerDetails()
    asdb.listTables()
    newTableNames = ['hf28','hf27','hf26','hf25','hf24']
    newTableFields = ["id integer identity(1,1)", "name varchar(32)", "value integer"]
    for t in newTableNames:
        asdb.createTableIfNotExists(t, newTableFields)

    for t in newTableNames:
        N = 5

        # the "value" value can be a stringified or non-stringified integer
        records = [{"name":f"{t}_mark", "value":i} for i in range(N)]
        records = [{"name":f"{t}_mark", "value":str(i)} for i in range(N)]

        # insert N records in the table one at a time
        #=============================================
        # for r in records:
        #     asdb.insertOne(t, **r)

        # insert N records in one transaction committed
        #=============================================
        # asdb.insertMany(t, records)

        # insert N records in one insert
        #=============================================
        asdb.insertMany2(t, records)

    asdb.listTables()

    for t in newTableNames:
        for v in range(N):
            r = asdb.selectWhere(t, f"value = {v};")

    for t in newTableNames:
        asdb.dropTableIfExists(t)


if __name__ == "__main__":
    main()