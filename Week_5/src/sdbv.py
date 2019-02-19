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
        translog = self.transactions[version_id]
        # this loop checks the current transaction log for the rowid, if it is not found, then the function proceeds
        # if it is found in the current transaction log, then returns that rowid as a row object
        # records in the transactions dictionary contain rawdata in their internal change variable
        for i in range((len(translog)-1), -1, -1):
            record = translog[i]
            if record.rowid == rowid:
                # special case. delete records do not have row data (string). instead, delete records have a byte type
                # if this was not here, when we return the row object using the byte type in delete.change, the row
                # object constructor will throw an error as byte is not a str, list, or tuple.
                if record.kind == 3:
                    return False
                # all other cases
                else:
                    # must return a row object, otherwise error "str does not have attribute values" occurs
                    return Row(self.schema, record.change)

        # check if the current versionid is greater than the history of that rowid. Example: if versiond = 1 (first
        # transaction) and the versionid of that row is 0 (original entry), then the entry in the database
        # will be returned. If the versionid of the working row is greater, or in other words, if the recorded rowid in
        # the database is newer (greater), then return the row from the history dictionary where its versionid is less
        # than the current transaction (versionid)
        if self.row_versionid[rowid] < version_id:  # return row object from database
            return self.sdb.getRow(rowid)
        else:
            for old_record in self.row_history[rowid]:
                if old_record.version_id < version_id:
                    return Row(self.schema, old_record.change)

    def insertRow(self, row, version_id):
        rowid = self.sdb.b1.findSpace(0)
        # check for full database
        if rowid == -1:
            return False

        self.sdb.b1.reserve(rowid)
        cr = ChangeRecord(version_id, ChangeRecord.INSERT, rowid, row.getRaw())
        self.transactions[version_id].append(cr)
        return rowid

    def deleteRow(self, rowid, version_id):
        cr = ChangeRecord(version_id, ChangeRecord.DELETE, rowid, b'')
        self.transactions[version_id].append(cr)
        return True

    def updateRow(self, rowid, new_row, version_id):
        cr = ChangeRecord(version_id, ChangeRecord.UPDATE, rowid, new_row.getRaw())
        self.transactions[version_id].append(cr)
        return True

    def startTransaction(self):
        trnid = self.getNextId()
        self.transactions[trnid] = []  # put trnid and empty list of change records into tran dictionary
        return trnid

    def getNextId(self):
        trnid = self.next_version_id
        self.next_version_id = self.next_version_id + 1
        return trnid

    def commit(self, version_id):
        try:
            # is transid in dict?
            if version_id not in self.transactions.keys():
                return False

            for record in self.transactions[version_id]:
                # check if record in translog is older than in database. if yes, rollback changes
                if not(self.row_versionid[record.rowid] < version_id):
                    if self.rollback(version_id):  # returns true if rollback was successful
                        return False  # don't commit anything, end function

                # create old change record
                old_record = ChangeRecord(version_id, ChangeRecord.BEFORE, record.rowid,
                                                  self.sdb.getRawRow(record.rowid))
                if record.rowid not in self.row_history.keys():  # create new key(rowid) and add old record
                    self.row_history[record.rowid] = [old_record]
                else:
                    self.row_history[record.rowid].insert(0, old_record)  # insert old record to first entry in rowid
                self.row_versionid[record.rowid] = self.getNextId()

                # proceed to change actual database
                if record.kind == 1:  # insert
                    self.sdb.insertRawRowId(record.rowid, record.change)
                    self.sdb.b1.__setitem__(record.rowid, 1)
                elif record.kind == 2:  # update
                    self.sdb.updateRawRow(record.rowid, record.change)
                elif record.kind == 3:  # delete
                    self.sdb.deleteRow(record.rowid)
            return True  # all changes executed successfully
        except:
            return False

    def rollback(self, version_id):
        for cr in self.transactions[version_id]:
            if cr.kind == ChangeRecord.INSERT:
                self.sdb.b1.unreserve(cr.rowid)
        del self.transactions[version_id]
        return True