import cx_Oracle

cx_Oracle.init_oracle_client(lib_dir="C:/instantclient_21_12")
## cx_Oracle.init_oracle_client(lib_dir="/Users/your_username/Downloads/instantclient_19_8")

print( cx_Oracle.clientversion()) 


connection = cx_Oracle.connect('hr', 'oracle', 'localhost:1521/FREEPDB1')

# Create a cursor object using the connection
cursor = connection.cursor()

# Now you can use this cursor to perform database operations.
# For example, to fetch all rows from the EMPLOYEES table:
cursor.execute('SELECT * FROM HR.EMPLOYEES')

# Fetch all the results
rows = cursor.fetchall()
for row in rows:
    print(row)

# Don't forget to close the cursor and connection when you're done
cursor.close()
connection.close()