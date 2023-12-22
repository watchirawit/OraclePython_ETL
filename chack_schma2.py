import cx_Oracle

# Replace these values with your Oracle connection details
username = 'your_username'
password = 'your_password'
dsn = 'your_dsn'  # Or use the connection string
cx_Oracle.init_oracle_client(lib_dir="C:/instantclient_21_12")
# Establish a connection to the Oracle database
connection = cx_Oracle.connect('hr', 'oracle', 'localhost:1521/FREEPDB1')

# Function to retrieve PK-FK relationships in the schema
def get_pk_fk_relationships():
    cursor = connection.cursor()

    # Dictionary to hold PK-FK relationships
    pk_fk_relationships = {}

    # Query to get all foreign keys in the schema
    cursor.execute("SELECT table_name, constraint_name, r_constraint_name FROM user_constraints WHERE constraint_type = 'R'")
    foreign_keys = cursor.fetchall()

    # Loop through each foreign key
    for table_name, constraint_name, r_constraint_name in foreign_keys:
        # Query to get the referenced and referencing tables
        cursor.execute(f"SELECT table_name FROM user_constraints WHERE constraint_name = '{r_constraint_name}'")
        referenced_table = cursor.fetchone()[0]

        # Adding PK table -> FK table relationship
        if referenced_table not in pk_fk_relationships:
            pk_fk_relationships[referenced_table] = []
        pk_fk_relationships[referenced_table].append((table_name, constraint_name))

    # Query to get all primary keys in the schema
    cursor.execute("SELECT table_name, constraint_name FROM user_constraints WHERE constraint_type = 'P'")
    primary_keys = cursor.fetchall()

    # Loop through each primary key
    for table_name, constraint_name in primary_keys:
        # Query to get the referencing tables
        cursor.execute(f"SELECT table_name, constraint_name FROM user_constraints WHERE r_constraint_name = '{constraint_name}'")
        referencing_tables = cursor.fetchall()

        # Adding FK table -> PK table relationship
        for ref_table_name, ref_constraint_name in referencing_tables:
            if table_name not in pk_fk_relationships:
                pk_fk_relationships[table_name] = []
            pk_fk_relationships[table_name].append((ref_table_name, ref_constraint_name))

    cursor.close()
    return pk_fk_relationships

# Get PK-FK relationships
relationships = get_pk_fk_relationships()

# Display PK-FK relationships
for pk_table, fk_tables in relationships.items():
    print(f"Primary Key Table: {pk_table}")
    print("Connected to Foreign Key Tables:")
    for fk_table, fk_constraint in fk_tables:
        print(f"- Foreign Key Table: {fk_table}, Constraint: {fk_constraint}")
    print("\n")

# Close the database connection
connection.close()
