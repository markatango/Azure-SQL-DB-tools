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

    def getServerDetails(self):
        print("override this function")

    def listTables(self):
        print("override this function")

    def createTableIfNotExists(self, tableName, fields):
       print("override this function")
 
    def dropTableIfExists(self, tableName):
        print("override this function")

    def insertOne(self,table, **fvPairs):
        print("override this function")

    def insertMany(self, table, fvList):
        print("override this function")

    def insertMany2(self, table, fvList):
        print("override this function")

    # select all five records in the table
    def selectWhere(self, table, condition):
        print("override this function")

