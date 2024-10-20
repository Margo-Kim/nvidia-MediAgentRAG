import sqlite3
import pandas as pd
import json

def drop_table(cursor, table_name):
    """
    Drops the specified table from the database.
    """
    drop_old_table_query = f"DROP TABLE IF EXISTS {table_name};"
    cursor.execute(drop_old_table_query)
    print(f"Old table '{table_name}' has been dropped (if existed).")

def rename_table(cursor, old_name, new_name):
    """
    Renames the specified table to a new name in the database.
    """
    rename_table_query = f"ALTER TABLE {old_name} RENAME TO {new_name};"
    cursor.execute(rename_table_query)
    print(f"Table '{old_name}' has been renamed to '{new_name}'.")

def check_table_schema(conn, table_name):
    """
    Prints the schema of the specified table in the database.
    """
    query = f"PRAGMA table_info({table_name});"
    table_info = pd.read_sql(query, conn)
    print(f"Schema of '{table_name}':")
    print(table_info)

def remove_duplicates_from_table(table_name):
    """
    Removes duplicate rows from a table by copying unique rows to a temporary table,
    dropping the original table, and renaming the temporary table back to the original name.
    """
    with sqlite3.connect('mimic.db') as conn:
        cursor = conn.cursor()

        # Create a temporary table to store unique records
        create_temp_table_query = f"""
        CREATE TABLE {table_name}_unique AS
        SELECT DISTINCT * FROM {table_name};
        """
        cursor.execute(create_temp_table_query)
        print(f"Temporary table '{table_name}_unique' created and unique data copied.")

        # Drop the original table
        drop_table(cursor, table_name)

        # Rename the temporary table to the original table's name
        rename_table(cursor, f"{table_name}_unique", table_name)

        print("All changes have been committed, and the database connection is closed.")

def remove_duplicates_from_table_with_key(table_name, key_columns):
    """
    Removes duplicate rows from the specified table based on key columns, 
    keeping only one record for each unique combination of the given key columns.
    """
    with sqlite3.connect('mimic.db') as conn:
        cursor = conn.cursor()

        # Create a temporary table with unique records based on the key columns
        key_columns_str = ", ".join(key_columns)
        create_temp_table_query = f"""
        CREATE TABLE {table_name}_unique AS
        SELECT * FROM {table_name}
        WHERE ROWID IN (
            SELECT MIN(ROWID)
            FROM {table_name}
            GROUP BY {key_columns_str}
        );
        """
        cursor.execute(create_temp_table_query)
        print(f"Temporary table '{table_name}_unique' created and unique data copied.")

        # Drop the original table
        drop_table(cursor, table_name)

        # Rename the temporary table to the original table's name
        rename_table(cursor, f"{table_name}_unique", table_name)

        print("All changes have been committed, and the database connection is closed.")

def export_dataframe_to_csv(dataframe, file_name):
    """
    Exports a DataFrame object to a CSV file.
    """
    try:
        dataframe.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"File has been successfully saved as {file_name}")
    except Exception as e:
        print(f"Error exporting CSV file: {e}")

def create_index(table_name, index_name, attributes):
    """
    Creates an index on specified attributes of a given table.
    """
    try:
        with sqlite3.connect('mimic.db') as conn:
            cursor = conn.cursor()

            # Drop the index if it already exists
            drop_index_query = f"DROP INDEX IF EXISTS {index_name};"
            cursor.execute(drop_index_query)

            # Prepare the SQL command to create an index
            attributes_str = ", ".join(attributes)
            create_index_query = f"CREATE INDEX {index_name} ON {table_name}({attributes_str});"

            # Execute the query
            cursor.execute(create_index_query)
            print(f"Index '{index_name}' created successfully on {attributes_str} in '{table_name}' table.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
