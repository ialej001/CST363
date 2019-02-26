# test case for index
# does query using index and compares result
# to query not using index.  Should be same.
from sdb import * 
from sdbfilter import * 
import random

resultRowsIndex = []
resultRows = []
testPass = True


# create schema.  id has unique index.  salary has non-unique index

c1 = Column("id", Column.INTEGER, index=True, unique=True)
c2 = Column("salary", Column.DOUBLE, index=True, unique=False)
c3 = Column("name", Column.TEXT, length=20,index=False, unique=False)
emp = Schema("employee2", (c1, c2, c3))
db = SimpleDB(emp)
db.create()

# generate some random data and insert the row into the database
for i in range(50):
    id = i 
    salary = random.uniform(10000,20000)
    if i%2==0:
        name = 'David S. P'+str(i)
    else:
        name = 'Tom C'+str(i)
    row = Row(db.schema, (id, salary, name))
    db.insertRow(row)
db.write()

#db.print(indexes=True)

print("search using salary index >=14500 and <=18500")
cursor = CursorIndex(db, Predicate(), "salary", 14500, 18500)
   
while cursor.next() >= 0:
    resultRowsIndex.append(cursor.getRow())
    
print("search using no index")
cursor = Cursor(db, 
                AndPredicate(Compare(db, "salary", 
                                        Compare.GE,
                                         14500),
                             Compare(db, "salary", 
                                         Compare.LE,
                                         18500)))

while cursor.next() >= 0:
    resultRows.append(cursor.getRow())
    
# now compare the result lists
if len(resultRowsIndex) != len(resultRows):
    print("Error.  results do not have same number of rows")
    print("Using index there are", len(resultRowsIndex), "rows returned.")
    #for row1 in resultRowsIndex:
    #    print(row1)
    print("Not using index there are", len(resultRows), "rows returned.")
    #for row2 in resultRows:
    #    print(row2)
    testPass = False
    
for row1 in resultRowsIndex:
    found=False
    for row2 in resultRows:
        if row1.values[0] == row2.values[0]:
            found=True
    if not found:
        print("Error. Index returned extra row", row1)
        testPass=False
        
for row1 in resultRows :
    found=False
    for row2 in resultRowsIndex:
        if row1.values[0] == row2.values[0]:
            found=True
    if not found:
        print("Error. No index returned extra row", row1)
        testPass=False
        
resultRowsIndex = []
resultRows = []

        
print("search using index,  id >=25 and id <=75")
cursor = CursorIndex(db, Predicate(), "id", 25, 75)
   
while cursor.next() >= 0:
    resultRowsIndex.append(cursor.getRow())
    
print("search using no index")
cursor = Cursor(db, 
                AndPredicate(Compare(db, "id", 
                                        Compare.GE,
                                         25),
                             Compare(db, "id", 
                                         Compare.LE,
                                         75)))

while cursor.next() >= 0:
    resultRows.append(cursor.getRow())
    
# now compare the result lists
if len(resultRowsIndex) != len(resultRows):
    print("Error.  results do not have same number of rows")
    print("Using index there are", len(resultRowsIndex), "rows returned.")
    #for row1 in resultRowsIndex:
    #    print(row1)
    print("Not using index there are", len(resultRows), "rows returned.")
    #for row2 in resultRows:
    #    print(row2)
    testPass = False
    
for row1 in resultRowsIndex:
    found=False
    for row2 in resultRows:
        if row1.values[0] == row2.values[0]:
            found=True
    if not found:
        print("Error. Index returned extra row", row1.values[0])
        testPass = False
        
for row1 in resultRows :
    found=False
    for row2 in resultRowsIndex:
        if row1.values[0] == row2.values[0]:
            found=True
    if not found:
        print("Error. No index returned extra row", row1.values[0])
        testPass = False

if testPass:
    print("All tests pass")
else:
    print("One or more tests failed")
    
print("End of index_test_2")




