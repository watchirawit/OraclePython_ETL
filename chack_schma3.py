import cx_Oracle
cx_Oracle.init_oracle_client(lib_dir="C:/instantclient_21_12")
# Replace these values with your Oracle connection details
username = 'your_username'
password = 'your_password'
dsn = 'your_dsn'  # Or use the connection string

# Establish a connection to the Oracle database
connection = cx_Oracle.connect('hr', 'oracle', 'localhost:1521/FREEPDB1')

# Function to retrieve PK-FK relationships and their attributes
def get_pk_fk_attributes():
    cursor = connection.cursor()

    # Dictionary to hold PK-FK relationships and their attributes
    pk_fk_attributes = {}

    # Query to get all foreign keys in the schema
    cursor.execute("SELECT table_name, constraint_name, r_constraint_name FROM user_constraints WHERE constraint_type = 'R'")
    foreign_keys = cursor.fetchall()

    # Loop through each foreign key
    for table_name, constraint_name, r_constraint_name in foreign_keys:
        # Query to get the column(s) involved in the foreign key constraint
        cursor.execute(f"SELECT column_name FROM user_cons_columns WHERE constraint_name = '{constraint_name}'")
        fk_columns = [row[0] for row in cursor.fetchall()]

        # Query to get the referenced table
        cursor.execute(f"SELECT table_name FROM user_constraints WHERE constraint_name = '{r_constraint_name}'")
        referenced_table = cursor.fetchone()[0]

        # Adding PK table -> FK table relationship and their attributes
        if referenced_table not in pk_fk_attributes:
            pk_fk_attributes[referenced_table] = []
        pk_fk_attributes[referenced_table].append((table_name, constraint_name, fk_columns))

    cursor.close()
    return pk_fk_attributes

# Get PK-FK relationships and their attributes
relationships_with_attributes = get_pk_fk_attributes()

# Display PK-FK relationships and their attributes
for pk_table, fk_tables_attributes in relationships_with_attributes.items():
    print(f"Primary Key Table: {pk_table}")
    print("Connected to Foreign Key Tables with Attributes:")
    for fk_table, fk_constraint, fk_columns in fk_tables_attributes:
        print(f"- Foreign Key Table: {fk_table}, Constraint: {fk_constraint}, Attributes Used: {', '.join(fk_columns)}")
    print("\n")

# Close the database connection
connection.close()
