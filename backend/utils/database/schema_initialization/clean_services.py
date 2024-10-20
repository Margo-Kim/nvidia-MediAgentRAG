import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_services():
        
    # Remove duplicates from the 'services' table
    remove_duplicates_from_table("services")
    
    remove_duplicates_from_table_with_key("services", ["hadm_id", "transfertime"])

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
        drop_table_query = "DROP TABLE IF EXISTS services_clean;"
        cursor.execute(drop_table_query)
        print(" 'services_clean' table dropped if existed. ")


        # Create a new table 'services_clean'
        create_table_query = """
        CREATE TABLE services_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            transfertime DATE,
            prev_service TEXT,
            curr_service TEXT,
            PRIMARY KEY (hadm_id, transfertime)
        );
        """

        cursor.execute(create_table_query)
        print("'services_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO services_clean (subject_id,
            hadm_id,
            transfertime,
            prev_service,
            curr_service)
        SELECT subject_id,
            hadm_id,
            transfertime,
            prev_service,
            curr_service
        FROM services;
        """
        cursor.execute(copy_data_query)
        print("'services' data is successfully copied to 'services_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "services")
        
        # Rename the new table to the original name
        rename_table(cursor, "services_clean", "services")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "services")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")