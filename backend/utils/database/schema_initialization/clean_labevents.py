import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_labevents():
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
        drop_table_query = "DROP TABLE IF EXISTS labevents_clean;"
        cursor.execute(drop_table_query)
        print(" 'labevents_clean' table dropped if existed. ")
        
        # create new table
        create_table_query = """
        CREATE TABLE labevents_clean (
            labevent_id INTEGER PRIMARY KEY,
            subject_id INTEGER,
            hadm_id INTEGER,
            specimen_id INTEGER,
            itemid INTEGER,
            order_provider_id TEXT,
            charttime DATE,
            storetime DATE,
            value TEXT,
            valuenum REAL,
            valueuom TEXT,
            ref_range_lower REAL,
            ref_range_upper REAL,
            flag TEXT,
            priority TEXT,
            comments TEXT
        );
        """
        cursor.execute(create_table_query)
        print(" 'labevents_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO labevents_clean (labevent_id,
            subject_id,
            hadm_id,
            specimen_id,
            itemid,
            order_provider_id,
            charttime,
            storetime,
            value,
            valuenum,
            valueuom,
            ref_range_lower,
            ref_range_upper,
            flag,
            priority,
            comments)
        SELECT labevent_id,
            subject_id,
            hadm_id,
            specimen_id,
            itemid,
            order_provider_id,
            charttime,
            storetime,
            value,
            valuenum,
            valueuom,
            ref_range_lower,
            ref_range_upper,
            flag,
            priority,
            comments
        FROM labevents;
        """
        cursor.execute(copy_data_query)
        print(" 'labevents' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "labevents")
        
        # Rename the new table to the original name
        rename_table(cursor, "labevents_clean", "labevents")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "labevents")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")