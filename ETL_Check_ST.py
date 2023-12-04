import cx_Oracle
import pyodbc


cx_Oracle.init_oracle_client(lib_dir="C:/instantclient_21_12")
## cx_Oracle.init_oracle_client(lib_dir="/Users/your_username/Downloads/instantclient_19_8")

print( cx_Oracle.clientversion()) 

# Connect to the Oracle database
oracle_conn = cx_Oracle.connect('hr', 'oracle', 'localhost:1521/FREEPDB1')
oracle_cursor = oracle_conn.cursor()

# Connect to the SQL Server database
sql_server_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                 'SERVER=PCSFE2INF;'
                                 'DATABASE=hr_main;'
                                 'UID=sa;'
                                 'PWD=123456')
sql_server_cursor = sql_server_conn.cursor()

# Retrieve the Oracle HR schema table structures
oracle_cursor.execute("""
SELECT table_name 
FROM user_tables
""")
tables = oracle_cursor.fetchall()

# This function is a placeholder for the conversion logic
# You will need to map Oracle data types to SQL Server data types and handle other discrepancies
def oracle_to_sql_server_datatype(oracle_datatype, data_length, data_precision, data_scale):
    # Basic mapping from Oracle to SQL Server data types
    mapping = {
        'VARCHAR2': 'NVARCHAR',
        'NUMBER': 'NUMERIC',
        'DATE': 'DATETIME',
        # ... add other data type mappings as necessary
    }
    
    # Default to the mapped type or fallback to NVARCHAR(MAX)
    sql_server_datatype = mapping.get(oracle_datatype.upper(), 'NVARCHAR(MAX)')
    
    # Adjust for specifics of the data type
    if sql_server_datatype == 'NUMERIC' and data_precision is not None:
        return f'{sql_server_datatype}({data_precision},{data_scale if data_scale is not None else 0})'
    elif sql_server_datatype == 'NVARCHAR' and data_length is not None:
        return f'{sql_server_datatype}({data_length})'
    else:
        return sql_server_datatype

# ...[existing code]...

# Generate and execute SQL Server table creation statements
for table in tables:
    table_name = table[0]
    oracle_cursor.execute(f"""
        SELECT column_name, data_type, data_length, data_precision, data_scale 
        FROM user_tab_columns 
        WHERE table_name = '{table_name}'
    """)
    columns = oracle_cursor.fetchall()

    create_table_sql = f"CREATE TABLE {table_name} ("
    col_defs = []

    for column in columns:
        column_name, oracle_datatype, data_length, data_precision, data_scale = column
        sql_server_datatype = oracle_to_sql_server_datatype(
            oracle_datatype, data_length, data_precision, data_scale
        )

        # Append column definition to the list
        col_defs.append(f"{column_name} {sql_server_datatype}")

    # Join all column definitions and remove trailing comma if exists
    create_table_sql += ", ".join(col_defs)
    create_table_sql += ");"
    
    try:
        # Execute the creation of the table on SQL Server
        sql_server_cursor.execute(create_table_sql)
        sql_server_conn.commit()
    except pyodbc.ProgrammingError as e:
        print(f"An error occurred while creating table {table_name}: {e}")
        print(f"SQL Command: {create_table_sql}")
        # Handle the error or exit

# ...[remaining code]...



# Close the Oracle cursor and connection
oracle_cursor.close()
oracle_conn.close()

# Close the SQL Server cursor and connection
sql_server_cursor.close()
sql_server_conn.close()
