#!/usr/bin/env python3

# Ivan Alejandre
# CST 363
# 1/14/2019

# this file must be in the /cgi-bin/ directory of the server
import cgitb , cgi
import mysql.connector

cgitb.enable()
form = cgi.FieldStorage()

#
#  code to get input values goes here
#

userID = form['userID'].value
password = form['password'].value
register = False
login = False

if "register" in form:
	register = True
elif "login" in form:
	login = True

print("Content-Type: text/html")    # HTML is following
print()                             # blank line required, end of headers
print("<html><body>")
qsql = 'select visits from login where userid = %s and password = %s'
insert_sql = 'insert into login (userid, password, visits) values (%s, %s, 1)'
update_sql = 'update login set visits = visits + 1 where userid=%s and password=%s'

# connect to database
cnx = mysql.connector.connect(user='root',
                                password='password',
                                database='cst363',
                                host='127.0.0.1')

#  code to do SQL goes here
cursor = cnx.cursor()
cursor.execute(qsql, (userID, password))
row = cursor.fetchone()
 
if login is True:
	if row is None: 
		# do nothing, need to register before logging in
		print('Please register before logging in.')
	else:
		# retrieve number of visits value from row and increment
		visit_number = row[0] + 1
		cursorb = cnx.cursor()
		cursorb.execute(update_sql, (userID, password))
		print('Thank you for visiting again.  This is visit number '+str(visit_number))
elif register is True:
	if row is None:
		# add userid/password to database
		cursorb = cnx.cursor()
		cursorb.execute(insert_sql, (userID, password))
		print('Thank you for registering. You may now login.')
	else:
		# already registered
		print('You have already registered. Go back to login.')
		
print("</body></html>")
cnx.commit()
cnx.close()  # close the connection 
