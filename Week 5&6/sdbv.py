from schema import *
from sdb import *
import traceback

class ChangeRecord():
    INSERT = 1
    UPDATE = 2
    DELETE = 3
    BEFORE = 4
    
    def __init__(self, version_id, kind, rowid, rawdata):
        self.version_id = version_id
        self.kind = kind  # INSERT, UPDATE or DELETE change record
        self.rowid = rowid
        self.change = rawdata
         

class SimpleDBV():
# SimpleDB with versioning concurrency
    def __init__(self, schema):
        self.sdb = SimpleDB(schema) 
        self.schema = self.sdb.schema
        self.row_versionid = [0] * 4096   # a version id for each row_versionid
        self.row_history = dict()       # key=rowid, value=list of committed change records
        self.transactions = dict()      # key=tranid (versionid), value = list of change records
        self.sdb.b1.unreserveAll()
        self.next_version_id = 1;
          
    def create(self):
        self.sdb.create()
          
    def write(self):
        self.sdb.write()
          
    def print(self, indexes=False):
        # for debug - print out the contents of database
        self.sdb.print(indexes)
          
    def getRow(self, rowid, version_id):
        return self.sdb.getRow(rowid)
 
    def insertRow(self, row, version_id):
        rowid = self.sdb.b1.findSpace(0)
        self.sdb.b1.reserve(rowid)
        cr = ChangeRecord(trnid, ChangeRecord.INSERT, rowid, row.getRaw())
        self.transactions[trnid].append(cr)
        return rowid
        #return self.sdb.insertRow(row)
          
    def deleteRow(self, rowid, version_id):
        cr = ChangeRecord(trnid, ChangeRecord.DELETE, rowid, b'')
        self.transactions[trnid].append(cr)
        return True
        #return self.sdb.deleteRow(rowid)
          
    def updateRow(self, rowid, new_row, version_id):
        cr = ChangeRecord(trnid, ChangeRecord.BEFORE, rowid, new_row.getRaw())
        self.transactions[trnid].append(cr)
        return True #self.sdb.updateRow(rowid, new_row)
          
    def startTransaction(self):
        trnid = self.getNextId()
        self.transactions[trnid] = []     # put trnid and empty list of change records into tran dictionary
        return trnid
     
    def getNextId(self):
        trnid = self.next_version_id
        self.next_version_id = self.next_version_id + 1 
        return trnid          
          
    def commit(self, version_id):
        return False
        
    def rollback(self, version_id):
        for cr in self.transactions[trnid]:
            if cr.kind == ChangeRecord.INSERT:
                self.sdb.b1.unreserve(cr.rowid)
            del self.transactions[trnid]
        #return True
        return True
        