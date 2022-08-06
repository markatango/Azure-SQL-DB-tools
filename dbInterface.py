import pyodbc 
import os
import time

class DBInterface(object):
    def __init__(self):
        self.cnxn = None

    def listRows(self, cursor):
        print("override this function")

    def connect(self):
        print("override this function")

    # Get sql server details
    def getServerDetails(self):
        print("override this function")

    # list tables
    def listTables(self):
        print("override this function")

    # Create a table in the new database
    def createTableIfNotExists(self, tableName, fields):
       print("override this function")

    # Drop a table 
    def dropTableIfExists(self, tableName):
        print("override this function")

    # insert a record
    def insertOne(self,table, **fvPairs):
        print("override this function")

    # insert many records
    def insertMany(self, table, fvList):
        print("override this function")
    # insert many records
    # records = [{"name":f"{t}_mark", "value":str(i)} for i in range(N)]
    def insertMany2(self, table, fvList):
       print("override this function")

    # select all five records in the table
    def selectWhere(self, table, condition):
        print("override this function")

