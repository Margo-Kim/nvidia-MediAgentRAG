import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_hcpcsevents():
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
        drop_table_query = "DROP TABLE IF EXISTS hcpcsevents_clean;"
        cursor.execute(drop_table_query)
        print(" 'hcpcsevents_clean' table dropped if existed. ")
        
        # create new table
        create_table_query = """
        CREATE TABLE hcpcsevents_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            chartdate DATE,
            hcpcs_cd TEXT,
            seq_num INTEGER,
            short_description TEXT,
            PRIMARY KEY (hadm_id, chartdate, hcpcs_cd, seq_num)
        );
        """
        cursor.execute(create_table_query)
        print(" 'hcpcsevents_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO hcpcsevents_clean (subject_id,
            hadm_id,
            chartdate,
            hcpcs_cd,
            seq_num,
            short_description)
        SELECT subject_id,
            hadm_id,
            chartdate,
            hcpcs_cd,
            seq_num,
            short_description
        FROM hcpcsevents;
        """
        cursor.execute(copy_data_query)
        print(" 'hcpcsevents' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "hcpcsevents")
        
        # Rename the new table to the original name
        rename_table(cursor, "hcpcsevents_clean", "hcpcsevents")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "hcpcsevents")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")