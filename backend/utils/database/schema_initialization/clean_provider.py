import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_provider():
        
    # Remove duplicates from the 'provider' table
    remove_duplicates_from_table("provider")
    
    try:
        # db connection
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS provider_clean;"
        cursor.execute(drop_table_query)
        print(" 'provider_clean' table dropped if existed. ")

        # Create a new table 'provider_clean'
        create_table_query = """
        CREATE TABLE provider_clean (
            provider_id TEXT PRIMARY KEY
        );
        """

        cursor.execute(create_table_query)
        print("'provider_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO provider_clean (provider_id)
        SELECT provider_id
        FROM provider;
        """
        cursor.execute(copy_data_query)
        print("'provider' data is successfully copied to 'provider_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "provider")
        
        # Rename the new table to the original name
        rename_table(cursor, "provider_clean", "provider")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "provider")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")