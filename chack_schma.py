import cx_Oracle
cx_Oracle.init_oracle_client(lib_dir="C:/instantclient_21_12")

# Replace these values with your Oracle connection details
username = 'your_username'
password = 'your_password'
dsn = 'your_dsn'  # Or use the connection string

# Establish a connection to the Oracle database
connection = cx_Oracle.connect('hr', 'oracle', 'localhost:1521/F')

# Function to retrieve table structure, PK, and FK information for all tables
def get_all_table_info():
    cursor = connection.cursor()

    # Retrieve all table names in the user's schema
    cursor.execute("SELECT table_name FROM user_tables")
    tables = cursor.fetchall()

    table_info = {}

    # Loop through each table
    for table_name in tables:
        table_name = table_name[0]
        cursor.execute(f"SELECT column_name, data_type FROM user_tab_columns WHERE table_name = '{table_name.upper()}'")
        columns = cursor.fetchall()

        cursor.execute(f"SELECT column_name FROM user_cons_columns WHERE constraint_name = (SELECT constraint_name FROM user_constraints WHERE table_name = '{table_name.upper()}' AND constraint_type = 'P')")
        primary_keys = cursor.fetchall()

        cursor.execute(f"SELECT column_name FROM user_cons_columns WHERE constraint_name IN (SELECT constraint_name FROM user_constraints WHERE table_name = '{table_name.upper()}' AND constraint_type = 'R')")
        foreign_keys = cursor.fetchall()

        table_info[table_name] = {
            'columns': columns,
            'primary_keys': primary_keys,
            'foreign_keys': foreign_keys
        }

    cursor.close()
    return table_info

# Get information for all tables
all_table_info = get_all_table_info()

# Display information for all tables
for table_name, info in all_table_info.items():
    print(f"Table: {table_name}")
    print("Columns:")
    for column, data_type in info['columns']:
        print(f"- Column: {column}, Data Type: {data_type}")

    print("Primary Key:")
    for pk_column in info['primary_keys']:
        print(f"- Primary Key Column: {pk_column[0]}")

    print("Foreign Keys:")
    for fk_column in info['foreign_keys']:
        print(f"- Foreign Key Column: {fk_column[0]}")
    
    print("\n")

# Close the database connection
connection.close()
