import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_d_icd_procedures():
        
    # Remove duplicates from the 'd_icd_procedures' table
    remove_duplicates_from_table("d_icd_procedures")
    
    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS d_icd_procedures_clean;"
        cursor.execute(drop_table_query)
        print(" 'd_icd_procedures_clean' table dropped if existed. ")

        # Create a new table 'd_icd_procedures_clean'
        create_table_query = """
        CREATE TABLE d_icd_procedures_clean (
            icd_code TEXT,
            icd_version INTEGER,
            long_title TEXT,
            PRIMARY KEY (icd_code, icd_version)
        );
        """

        cursor.execute(create_table_query)
        print("'d_icd_procedures_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO d_icd_procedures_clean (icd_code,
            icd_version,
            long_title)
        SELECT icd_code,
            icd_version,
            long_title
        FROM d_icd_procedures;
        """
        cursor.execute(copy_data_query)
        print("'d_icd_procedures' data is successfully copied to 'd_icd_procedures_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "d_icd_procedures")
        
        # Rename the new table to the original name
        rename_table(cursor, "d_icd_procedures_clean", "d_icd_procedures")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "d_icd_procedures")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")