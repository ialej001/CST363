
from sdbv import * 
import unittest


class  Vc_grading_tesstTestCase(unittest.TestCase):
    
    def setUp(self):
        # this method is executed before each test
        c1 = Column("id", Column.INTEGER)
        c2 = Column("name", Column.TEXT, 30)
        c3 = Column("major", Column.TEXT, 30)
        c4 = Column("credits", Column.INTEGER) 
        schema = Schema("student", [c1, c2, c3, c4])
        self.db = SimpleDBV(schema)
        self.db.create()
        
    def test1(self):  # test  insert of row and commit
        trnid1=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid1)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid1)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid1)
        self.assertTrue(self.db.commit(trnid1))
        
        # check that rows exist
        trnid2 = self.db.startTransaction()
        row = self.db.getRow(rowid1, trnid2)
        self.assertEqual(row.values[0], 10)
        row = self.db.getRow(rowid2, trnid2)
        self.assertEqual(row.values[0], 20)
        row = self.db.getRow(rowid3, trnid2)
        self.assertEqual(row.values[0], 30)
        self.assertTrue(self.db.commit(trnid2))
        
    def test2(self):  # test update commit and update rollback 
        trnid1=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid1)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid1)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid1)
        self.assertTrue(self.db.commit(trnid1))
 
        
        # start tran 4 and update all rows and commit
        trnid4 = self.db.startTransaction()
        row = self.db.getRow(rowid1, trnid4)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid1, row, trnid4)
        row = self.db.getRow(rowid2, trnid4)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid2, row, trnid4)
        row = self.db.getRow(rowid3, trnid4)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid3, row, trnid4)
        self.assertTrue(self.db.commit(trnid4))
        
        # start tran 5, update all rows and rollback
        trnid5 = self.db.startTransaction()
        row = self.db.getRow(rowid1, trnid5)
        self.assertEqual(row.values[3], 20)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid1, row, trnid5)
        row = self.db.getRow(rowid2, trnid5)
        self.assertEqual(row.values[3], 35)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid2, row, trnid5)
        row = self.db.getRow(rowid3, trnid5)
        self.assertEqual(row.values[3], 55)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid3, row, trnid5)
        self.assertTrue(self.db.rollback(trnid5))
        
        # start tran 6, check values of rows
        trnid6 = self.db.startTransaction()
        row = self.db.getRow(rowid1, trnid6)
        self.assertEqual(row.values[3], 20)
        row = self.db.getRow(rowid2, trnid6)
        self.assertEqual(row.values[3], 35)
        row = self.db.getRow(rowid3, trnid6)
        self.assertEqual(row.values[3], 55)
        self.assertTrue(self.db.commit(trnid6))
        
  
    def test3(self):   # test repeatable read isolation
        trnid1=self.db.startTransaction()
        rowid1 = self.db.insertRow(Row(self.db.schema, [10, "tom", "cs", 10]), trnid1)
        rowid2 = self.db.insertRow(Row(self.db.schema, [20, "david", "math", 25]), trnid1)
        rowid3 = self.db.insertRow(Row(self.db.schema, [30, "ana", "cs", 45]), trnid1)
        self.assertTrue(self.db.commit(trnid1))
 
        # start transaction 3
        trnid3 = self.db.startTransaction()
        
        # start tran 4 and update all rows and commit
        trnid4 = self.db.startTransaction()
        row = self.db.getRow(rowid1, trnid4)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid1, row, trnid4)
        row = self.db.getRow(rowid2, trnid4)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid2, row, trnid4)
        row = self.db.getRow(rowid3, trnid4)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid3, row, trnid4)
        self.assertTrue(self.db.commit(trnid4))
        
        # start tran 5, update all rows and rollback
        trnid5 = self.db.startTransaction()
        row = self.db.getRow(rowid1, trnid5)
        self.assertEqual(row.values[3], 20)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid1, row, trnid5)
        row = self.db.getRow(rowid2, trnid5)
        self.assertEqual(row.values[3], 35)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid2, row, trnid5)
        row = self.db.getRow(rowid3, trnid5)
        self.assertEqual(row.values[3], 55)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid3, row, trnid5)
        self.assertTrue(self.db.rollback(trnid5))
        
        # start tran 6, update all rows and commit
        trnid6 = self.db.startTransaction()
        row = self.db.getRow(rowid1, trnid6)
        self.assertEqual(row.values[3], 20)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid1, row, trnid6)
        row = self.db.getRow(rowid2, trnid6)
        self.assertEqual(row.values[3], 35)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid2, row, trnid6)
        row = self.db.getRow(rowid3, trnid6)
        self.assertEqual(row.values[3], 55)
        row.values[3] = row.values[3] + 10
        self.db.updateRow(rowid3, row, trnid6)
        self.assertTrue(self.db.commit(trnid6))
        
        # tran 3 should still get the original values of the rows
        row = self.db.getRow(rowid1, trnid3)
        #print(rowid1)
        self.assertEqual(row.values[3], 10)
        row = self.db.getRow(rowid2, trnid3)
        self.assertEqual(row.values[3], 25)
        row = self.db.getRow(rowid3, trnid3)
        self.assertEqual(row.values[3], 45)
        self.assertTrue(self.db.commit(trnid3))

if __name__ == '__main__':
    unittest.main()

