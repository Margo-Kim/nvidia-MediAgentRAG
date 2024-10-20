import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_procedureevents():
        
    # Remove duplicates from the 'procedureevents' table
    remove_duplicates_from_table("procedureevents")

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
        drop_table_query = "DROP TABLE IF EXISTS procedureevents_clean;"
        cursor.execute(drop_table_query)
        print(" 'procedureevents_clean' table dropped if existed. ")

        # Create a new table 'procedureevents_clean'
        create_table_query = """
        CREATE TABLE procedureevents_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            stay_id INTEGER,
            caregiver_id INTEGER,
            starttime DATE,
            endtime DATE,
            storetime DATE,
            itemid INTEGER,
            value REAL,
            valueuom TEXT,
            location TEXT,
            locationcategory TEXT,
            orderid INTEGER,
            linkorderid INTEGER,
            ordercategoryname TEXT,
            ordercategorydescription TEXT,
            patientweight REAL,
            isopenbag INTEGER,
            continueinnextdept INTEGER,
            statusdescription TEXT,
            originalamount REAL,
            originalrate REAL,
            PRIMARY KEY (orderid, itemid)
        );
        """

        cursor.execute(create_table_query)
        print("'procedureevents_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO procedureevents_clean (subject_id,
            hadm_id,
            stay_id,
            caregiver_id,
            starttime,
            endtime,
            storetime,
            itemid,
            value,
            valueuom,
            location,
            locationcategory,
            orderid,
            linkorderid,
            ordercategoryname,
            ordercategorydescription,
            patientweight,
            isopenbag,
            continueinnextdept,
            statusdescription,
            originalamount,
            originalrate)
        SELECT subject_id,
            hadm_id,
            stay_id,
            caregiver_id,
            starttime,
            endtime,
            storetime,
            itemid,
            value,
            valueuom,
            location,
            locationcategory,
            orderid,
            linkorderid,
            ordercategoryname,
            ordercategorydescription,
            patientweight,
            isopenbag,
            continueinnextdept,
            statusdescription,
            originalamount,
            originalrate
        FROM procedureevents;
        """
        cursor.execute(copy_data_query)
        print("'procedureevents' data is successfully copied to 'procedureevents_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "procedureevents")
        
        # Rename the new table to the original name
        rename_table(cursor, "procedureevents_clean", "procedureevents")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "procedureevents")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")