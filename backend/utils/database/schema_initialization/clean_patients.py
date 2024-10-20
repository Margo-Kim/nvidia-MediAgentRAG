import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_patients():
    # Remove duplicates from the 'patients' table
    remove_duplicates_from_table("patients")
    
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
        drop_table_query = "DROP TABLE IF EXISTS patients_clean;"
        cursor.execute(drop_table_query)
        print(" 'patients_clean' table dropped if existed. ")
        
        # Create new table 'patients_clean'
        create_table_query = """
        CREATE TABLE patients_clean (
            subject_id INTEGER PRIMARY KEY,
            gender TEXT,
            anchor_age INTEGER,
            anchor_year INTEGER,
            anchor_year_group TEXT,
            dod DATE
        );
        """
        cursor.execute(create_table_query)
        print("'patients_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO patients_clean (subject_id, gender, anchor_age, anchor_year, anchor_year_group, dod)
        SELECT subject_id, gender, anchor_age, anchor_year, anchor_year_group, dod FROM patients;
        """
        cursor.execute(copy_data_query)
        print("'patients' is successfully copied.")
        
        # Drop the old table using the reusable function
        drop_table(cursor, "patients")
        
        # Rename the new table to the original name
        rename_table(cursor, "patients_clean", "patients")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "patients")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")