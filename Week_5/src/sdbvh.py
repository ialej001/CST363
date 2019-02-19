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
        self.row_versionid = [0] * 4096  # a version id for each row_versionid
        self.row_history = dict()  # key=rowid, value=list of committed change records

        # The key values will be the transaction number
        # and the value will be a list of ChangeRecord objects for the transaction
        self.transactions = dict()  # key=tranid (versionid), value = list of change records
        self.sdb.b1.unreserveAll()
        self.next_version_id = 1

    def create(self):
        self.sdb.create()

    def write(self):
        self.sdb.write()

    def print(self, indexes=False):
        # for debug - print out the contents of database
        self.sdb.print(indexes)

    def getRow(self, rowid, version_id):
        trnlog = self.transactions[version_id]
        for i in range(len(trnlog) - 1, -1, -1):
            cr = trnlog[i]
            if cr.rowid == rowid:
                # self.commit(version_id)
                return cr.change
        if self.row_versionid[rowid] < version_id:
            return self.sdb.getRow(rowid)
        else:
            # searchRowHistory
            return self.__getRowHistoryRow__(rowid, version_id)

    def __getRowHistoryRow__(self, rowid, version_id):
        for h in self.row_history[rowid]:
            if h.version_id < version_id:
                return h.change

    def insertRow(self, row, version_id):
        rowid = self.sdb.b1.findSpace(0)
        self.sdb.b1.reserve(rowid)
        cr = ChangeRecord(version_id, ChangeRecord.INSERT, rowid, row.getRaw())
        self.transactions[version_id].append(cr)
        return rowid
        # return self.sdb.insertRow(row)

    def deleteRow(self, rowid, version_id):
        cr = ChangeRecord(version_id, ChangeRecord.DELETE, rowid, b'')
        self.transactions[version_id].append(cr)
        return True
        # return self.sdb.deleteRow(rowid)

    def updateRow(self, rowid, new_row, version_id):
        cr = ChangeRecord(version_id, ChangeRecord.UPDATE, rowid, new_row.getRaw())
        self.transactions[version_id].append(cr)
        return True
        # return self.sdb.updateRow(rowid, new_row)

    def startTransaction(self):
        # code for startTransaction function creates a dictionary entry with a key value and empty list.
        trnid = self.getNextId()
        self.transactions[trnid] = []  # put trnid and empty list of change records into tran dictionary
        return trnid

    def getNextId(self):
        trnid = self.next_version_id
        self.next_version_id = self.next_version_id + 1
        return trnid

    def commit(self, version_id):
        try:
            execList = self.transactions[version_id]
            for cr in execList:

                if not self.row_versionid[cr.rowid] < version_id:
                    self.rollback(cr.version_id)
                    return False

                self.__addHistory__(cr)
                if cr.kind == cr.INSERT:
                    self.sdb.insertRawRowId(cr.rowid, cr.change)
                    self.sdb.b1.set(cr.rowid)
                if cr.kind == cr.UPDATE:
                    self.sdb.updateRawRow(cr.rowid, cr.change)
                if cr.kind == cr.DELETE:
                    self.sdb.deleteRow(cr.rowid)
            return True
        except:
            return False

    def rollback(self, version_id):
        for cr in self.transactions[version_id]:
            if cr.kind == ChangeRecord.INSERT:
                self.sdb.b1.unreserve(cr.rowid)
        del self.transactions[version_id]
        return True

    def __addHistory__(self, changeRec):
        if changeRec.rowid not in self.row_history.keys():
            self.row_history[changeRec.rowid] = []

        changeObj = ChangeRecord(self.row_versionid[changeRec.rowid], changeRec.kind, changeRec.rowid,
                                 self.sdb.getRow(changeRec.rowid))

        self.row_history[changeRec.rowid].insert(0, changeObj)
        self.row_versionid[changeRec.rowid] = self.getNextId()  # changeRec.version_id + 1
