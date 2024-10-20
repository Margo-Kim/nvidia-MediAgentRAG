import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_d_hcpcs():
    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS d_hcpcs_clean;"
        cursor.execute(drop_table_query)
        print(" 'd_hcpcs_clean' table dropped if existed. ")

        # create new table
        create_table_query = """
        CREATE TABLE d_hcpcs_clean (
            code TEXT PRIMARY KEY,
            category INTEGER,
            long_description TEXT,
            short_description TEXT
        );
        """
        cursor.execute(create_table_query)
        print(" 'd_hcpcs_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO d_hcpcs_clean (code,
            category,
            long_description,
            short_description)
        SELECT code,
            category,
            long_description,
            short_description
        FROM d_hcpcs;
        """
        cursor.execute(copy_data_query)
        print(" 'd_hcpcs' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "d_hcpcs")
        
        # Rename the new table to the original name
        rename_table(cursor, "d_hcpcs_clean", "d_hcpcs")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "d_hcpcs")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")