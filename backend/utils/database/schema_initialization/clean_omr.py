import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_omr():
        
    # Remove duplicates from the 'omr' table
    remove_duplicates_from_table("omr")
    
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
        drop_table_query = "DROP TABLE IF EXISTS omr_clean;"
        cursor.execute(drop_table_query)
        print(" 'omr_clean' table dropped if existed. ")

        # Create a new table 'omr_clean'
        create_table_query = """
        CREATE TABLE omr_clean (
            subject_id INTEGER,
            chartdate DATE,
            seq_num INTEGER,
            result_name TEXT,
            result_value TEXT,
            PRIMARY KEY (subject_id, chartdate, seq_num, result_name)
        );
        """

        cursor.execute(create_table_query)
        print("'omr_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO omr_clean (subject_id,
            chartdate,
            seq_num,
            result_name,
            result_value)
        SELECT
            subject_id,
            chartdate,
            seq_num,
            result_name,
            result_value
        FROM omr;
        """
        cursor.execute(copy_data_query)
        print("'omr' data is successfully copied to 'omr_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "omr")
        
        # Rename the new table to the original name
        rename_table(cursor, "omr_clean", "omr")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "omr")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")