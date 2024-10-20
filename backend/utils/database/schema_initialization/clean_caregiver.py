import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_caregiver():
        
    # Remove duplicates from the 'caregiver' table
    remove_duplicates_from_table("caregiver")

    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS caregiver_clean;"
        cursor.execute(drop_table_query)
        print(" 'caregiver_clean' table dropped if existed. ")

        # Create a new table 'caregiver_clean'
        create_table_query = """
        CREATE TABLE caregiver_clean (
            caregiver_id TEXT PRIMARY KEY
        );
        """

        cursor.execute(create_table_query)
        print("'caregiver_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO caregiver_clean (caregiver_id)
        SELECT caregiver_id
        FROM caregiver;
        """
        cursor.execute(copy_data_query)
        print("'caregiver' data is successfully copied to 'caregiver_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "caregiver")
        
        # Rename the new table to the original name
        rename_table(cursor, "caregiver_clean", "caregiver")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "caregiver")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")