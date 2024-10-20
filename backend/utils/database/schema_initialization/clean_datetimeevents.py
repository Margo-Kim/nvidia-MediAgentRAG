import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_datetimeevents():
        
    # Remove duplicates from the 'datetimeevents' table
    remove_duplicates_from_table("datetimeevents")

    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS datetimeevents_clean;"
        cursor.execute(drop_table_query)
        print(" 'datetimeevents_clean' table dropped if existed. ")


        # Create a new table 'datetimeevents_clean'
        create_table_query = """
        CREATE TABLE datetimeevents_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            stay_id INTEGER,
            caregiver_id INTEGER,
            charttime DATE,
            storetime DATE,
            itemid INTEGER,
            value REAL,
            valueuom TEXT,
            warning INTEGER,
            PRIMARY KEY (stay_id, itemid, charttime)
        );
        """

        cursor.execute(create_table_query)
        print("'datetimeevents_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO datetimeevents_clean (subject_id,
            hadm_id,
            stay_id,
            caregiver_id,
            charttime,
            storetime,
            itemid,
            value,
            valueuom,
            warning)
        SELECT subject_id,
            hadm_id,
            stay_id,
            caregiver_id,
            charttime,
            storetime,
            itemid,
            value,
            valueuom,
            warning
        FROM datetimeevents;
        """
        cursor.execute(copy_data_query)
        print("'datetimeevents' data is successfully copied to 'datetimeevents_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "datetimeevents")
        
        # Rename the new table to the original name
        rename_table(cursor, "datetimeevents_clean", "datetimeevents")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "datetimeevents")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")