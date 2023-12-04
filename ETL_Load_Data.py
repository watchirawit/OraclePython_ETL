import cx_Oracle
import pyodbc

cx_Oracle.init_oracle_client(lib_dir="C:/instantclient_21_12")
## cx_Oracle.init_oracle_client(lib_dir="/Users/your_username/Downloads/instantclient_19_8")

print( cx_Oracle.clientversion()) 

def transfer_data(oracle_cursor, sql_server_cursor, table_name):
    # Fetch column names from Oracle
    oracle_cursor.execute(f"SELECT column_name FROM user_tab_columns WHERE table_name = '{table_name}'")
    columns = [row[0] for row in oracle_cursor.fetchall()]
    columns_list = ", ".join(columns)

    # Fetch data from Oracle
    oracle_cursor.execute(f"SELECT {columns_list} FROM {table_name}")
    rows = oracle_cursor.fetchall()

    # Prepare the INSERT statement for SQL Server
    placeholders = ', '.join(['?' for _ in columns])
    insert_sql = f"INSERT INTO {table_name} ({columns_list}) VALUES ({placeholders})"

    # Insert data into SQL Server
    for row in rows:
        sql_server_cursor.execute(insert_sql, row)

# Oracle connection
oracle_conn = cx_Oracle.connect('hr', 'oracle', 'localhost:1521/FREEPDB1')
oracle_cursor = oracle_conn.cursor()

# SQL Server connection
sql_server_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                 'SERVER=PCSFE2INF;'
                                 'DATABASE=hr_main;'
                                 'UID=sa;'
                                 'PWD=123456')
sql_server_cursor = sql_server_conn.cursor()

# Transfer data for each table
oracle_cursor.execute("SELECT table_name FROM user_tables")
tables = oracle_cursor.fetchall()

for table in tables:
    table_name = table[0]
    transfer_data(oracle_cursor, sql_server_cursor, table_name)

# Commit and close connections
sql_server_conn.commit()
oracle_cursor.close()
oracle_conn.close()
sql_server_cursor.close()
sql_server_conn.close()
