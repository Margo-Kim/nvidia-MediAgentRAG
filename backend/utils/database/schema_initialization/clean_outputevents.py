import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_outputevents():
        
    # Remove duplicates from the 'outputevents' table
    remove_duplicates_from_table("outputevents")

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
        drop_table_query = "DROP TABLE IF EXISTS outputevents_clean;"
        cursor.execute(drop_table_query)
        print(" 'outputevents_clean' table dropped if existed. ")

        # Create a new table 'outputevents_clean'
        create_table_query = """
        CREATE TABLE outputevents_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            stay_id INTEGER,
            caregiver_id INTEGER,
            charttime DATE,
            storetime DATE,
            itemid INTEGER,
            value REAL,
            valueuom TEXT,
            PRIMARY KEY (stay_id, itemid, charttime)
        );
        """

        cursor.execute(create_table_query)
        print("'outputevents_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO outputevents_clean (subject_id,
            hadm_id,
            stay_id,
            caregiver_id,
            charttime,
            storetime,
            itemid,
            value,
            valueuom)
        SELECT subject_id,
            hadm_id,
            stay_id,
            caregiver_id,
            charttime,
            storetime,
            itemid,
            value,
            valueuom
        FROM outputevents;
        """
        cursor.execute(copy_data_query)
        print("'outputevents' data is successfully copied to 'outputevents_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "outputevents")
        
        # Rename the new table to the original name
        rename_table(cursor, "outputevents_clean", "outputevents")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "outputevents")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")