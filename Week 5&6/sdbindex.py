#!/usr/bin/env python3
#encoding: windows-1252
class IndexEntryU:  # unique index entry
    # IndexEntry contains a value (an integer, double or string value) and a rowid 
    def __init__(self, value, rowid):
        self.value = value
        self.rowid = rowid
        
class IndexEntryNU:  # non-unique index entry
    # IndexEntry contains a value (an integer, double or string value) and a rowid 
    def __init__(self, value):
        self.value = value
        self.rowids = []
        
class Index:
    # An index consists of a list of IndexEntry objects that are in order by their value
    # To define an index specify the database object, the column name and whether the index is UNIQUE
    # after defining an index, you must call the create method to build the index entries
    def __init__(self, db, colname, colindex):
        self.db = db
        self.entries = []            # list of IndexEntry instances    
        self.colindex = colindex     # index into column list in schema
        self.colname = colname       # column name
 

    def print(self):
        # for debug - print content of index
        #print("Index", self.db.schema.cols[self.colindex].colname, "Non-Unique:")
        print("number of entries", len(self.entries))
        for entry in self.entries:
            print(entry.value, entry.rowids)
        print("Index end.")

    def create(self):
        for rowid in range(4096):
            row = self.db.getRow(rowid)
            if row != False:
                value = row.values[self.colindex]
                self.insert(rowid, value)

    def delete(self, rowid, value):
        count = 0
        for index in self.entries:
            if index.value == value and rowid in index.rowids:
                index.rowids.remove(rowid)
                if not index.rowids:
                    del self.entries[count]
                break
            count += 1
        pass
 
    def insert(self, rowid, value):
        newEntry = IndexEntryNU(value)
        newEntry.rowids.append(rowid)
        if not self.entries:  # empty array
            self.entries.insert(rowid, newEntry)
        else:
            for i in range(len(self.entries)):
                if value == self.entries[i].value:
                    self.entries[i].rowids.append(rowid)
                    break
                if value < self.entries[i].value:
                    self.entries.insert(i, newEntry)
                    break
        pass
    
    def search(self, value):
        # return index value of first IndexEntry 
        #         where IndexEntry.value >= value
        # only care if the searched value is >= to index value
        first = 0
        last = len(self.entries) - 1
        mid = (first + last) // 2

        while first <= last:
            if first == 0 and last == 1:
                return first
            elif self.entries[mid].value == value:  # found value
                return mid

            if value > self.entries[mid].value:  # go right
                first = mid+1
                mid = (first + last) // 2
            elif value < self.entries[mid].value:  # go left
                if self.entries[mid-1].value < value < self.entries[mid+1].value:
                    return mid
                else:
                    last = mid-1
                    mid = (first + last) // 2
        # return -1 if value is higher than all entries in index
        return -1
            
class UniqueIndex(Index):
    # this class is code unique to index that does not allow duplicate value entries
        def __init__(self, db, colname, colindex):
            super().__init__(db, colname, colindex)
        
        def print(self):
            # for debug - print content of index s
            #print("Index", self.db.schema.cols[self.colindex].colname, "Unique:")
            print("number of entries", len(self.entries))
            for entry in self.entries:
                print(entry.value, entry.rowid)
            print("Index end.")
        
        def insert(self, rowid, value):
            newEntry = IndexEntryU(value, rowid)
            if not self.entries:  # empty array
                self.entries.insert(rowid, newEntry)
            else:
                for i in range(len(self.entries)):
                    if value == self.entries[i].value:
                        raise ValueError("Value already exists: %d" % value)
                    if value < self.entries[i].value:#
                        self.entries.insert(i, newEntry)
                        break
            pass
    
        def delete(self, rowid, value):
            for index in self.entries:
                count = 0
                if index.value == value:# and index.rowid == rowid:
                    del self.entries[count]
                    break
                count += 1
            pass
        
