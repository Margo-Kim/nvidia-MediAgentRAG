import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_transfers():
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
        drop_table_query = "DROP TABLE IF EXISTS transfers_clean;"
        cursor.execute(drop_table_query)
        print(" 'transfers_clean' table dropped if existed. ")
        
        # create new table
        create_table_query = """
        CREATE TABLE transfers_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            transfer_id INTEGER PRIMARY KEY,
            eventtype TEXT,
            careunit TEXT,
            intime DATE,
            outtime DATE
        );
        """
        cursor.execute(create_table_query)
        print(" 'transfers_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO transfers_clean (subject_id,
            hadm_id,
            transfer_id,
            eventtype,
            careunit,
            intime,
            outtime)
        SELECT subject_id,
            hadm_id,
            transfer_id,
            eventtype,
            careunit,
            intime,
            outtime
        FROM transfers;
        """
        cursor.execute(copy_data_query)
        print(" 'transfers' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "transfers")
        
        # Rename the new table to the original name
        rename_table(cursor, "transfers_clean", "transfers")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "transfers")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")