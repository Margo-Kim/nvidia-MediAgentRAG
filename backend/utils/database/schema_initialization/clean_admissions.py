import pandas as pd
import sqlite3
import glob
import os
import sys 

from schema_initialization.data_utils import *

def clean_admissions():
    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS admissions_clean;"
        cursor.execute(drop_table_query)
        print(" 'admissions_clean' table dropped if existed. ")
        
        # Create new table
        create_table_query = """
        CREATE TABLE admissions_clean (
            subject_id INTEGER,
            hadm_id INTEGER PRIMARY KEY,
            admittime DATE,
            dischtime DATE,
            deathtime DATE,
            admission_type TEXT,
            admit_provider_id TEXT,
            admission_location TEXT,
            discharge_location TEXT,
            insurance TEXT,
            language TEXT,
            marital_status TEXT,
            race TEXT,
            edregtime DATE,
            edouttime DATE,
            hospital_expire_flag INTEGER
        );
        """
        cursor.execute(create_table_query)
        print(" 'admissions_clean' table is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO admissions_clean (subject_id,
            hadm_id,
            admittime,
            dischtime,
            deathtime,
            admission_type,
            admit_provider_id,
            admission_location,
            discharge_location,
            insurance,
            language,
            marital_status,
            race,
            edregtime,
            edouttime,
            hospital_expire_flag)
        SELECT subject_id,
            hadm_id,
            admittime,
            dischtime,
            deathtime,
            admission_type,
            admit_provider_id,
            admission_location,
            discharge_location,
            insurance,
            language,
            marital_status,
            race,
            edregtime,
            edouttime,
            hospital_expire_flag
        FROM admissions;
        """
        cursor.execute(copy_data_query)
        print(" 'admissions' is successfully copied. ")
        
        # Drop the old table using the reusable function
        drop_table(cursor, "admissions")
        
        # Rename the new table to the original name
        rename_table(cursor, "admissions_clean", "admissions")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "admissions")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")