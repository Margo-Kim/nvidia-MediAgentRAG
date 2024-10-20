import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_drgcodes():
        
    # Remove duplicates from the 'drgcodes' table
    remove_duplicates_from_table("drgcodes")
    
    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS drgcodes_clean;"
        cursor.execute(drop_table_query)
        print(" 'drgcodes_clean' table dropped if existed. ")

        # Create a new table 'drgcodes_clean'
        create_table_query = """
        CREATE TABLE drgcodes_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            drg_type TEXT,
            drg_code TEXT,
            description TEXT,
            drg_severity INTEGER,
            drg_mortality INTEGER,
            PRIMARY KEY (hadm_id, drg_type, drg_code)
        );
        """

        cursor.execute(create_table_query)
        print("'drgcodes_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO drgcodes_clean (subject_id,
            hadm_id,
            drg_type,
            drg_code,
            description,
            drg_severity,
            drg_mortality)
        SELECT subject_id,
            hadm_id,
            drg_type,
            drg_code,
            description,
            drg_severity,
            drg_mortality
        FROM drgcodes;
        """
        cursor.execute(copy_data_query)
        print("'drgcodes' data is successfully copied to 'drgcodes_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "drgcodes")
        
        # Rename the new table to the original name
        rename_table(cursor, "drgcodes_clean", "drgcodes")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "drgcodes")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")