import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_inputevents():
        
    # Remove duplicates from the 'inputevents' table
    remove_duplicates_from_table("inputevents")

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
        drop_table_query = "DROP TABLE IF EXISTS inputevents_clean;"
        cursor.execute(drop_table_query)
        print(" 'inputevents_clean' table dropped if existed. ")

        # Create a new table 'inputevents_clean'
        create_table_query = """
        CREATE TABLE inputevents_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            stay_id INTEGER,
            caregiver_id INTEGER,
            starttime DATE,
            endtime DATE,
            storetime DATE,
            itemid INTEGER,
            amount REAL,
            amountuom TEXT,
            rate REAL,
            rateuom TEXT,
            orderid INTEGER,
            linkorderid INTEGER,
            ordercategoryname TEXT,
            secondaryordercategoryname TEXT,
            ordercomponenttypedescription TEXT,
            ordercategorydescription TEXT,
            patientweight REAL,
            totalamount REAL,
            totalamountuom TEXT,
            isopenbag INTEGER,
            statusdescription TEXT,
            originalamount REAL,
            originalrate REAL,
            PRIMARY KEY (orderid,itemid)
        );
        """

        cursor.execute(create_table_query)
        print("'inputevents_clean' is successfully created.")
        
        # Copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO inputevents_clean (subject_id,
            hadm_id,
            stay_id,
            caregiver_id,
            starttime,
            endtime,
            storetime,
            itemid,
            amount,
            amountuom,
            rate,
            rateuom,
            orderid,
            linkorderid,
            ordercategoryname,
            secondaryordercategoryname,
            ordercomponenttypedescription,
            ordercategorydescription,
            patientweight,
            totalamount,
            totalamountuom,
            isopenbag,
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
            amount,
            amountuom,
            rate,
            rateuom,
            orderid,
            linkorderid,
            ordercategoryname,
            secondaryordercategoryname,
            ordercomponenttypedescription,
            ordercategorydescription,
            patientweight,
            totalamount,
            totalamountuom,
            isopenbag,
            statusdescription,
            originalamount,
            originalrate
        FROM inputevents;
        """
        cursor.execute(copy_data_query)
        print("'inputevents' data is successfully copied to 'inputevents_clean'.")

        # Drop the old table using the reusable function
        drop_table(cursor, "inputevents")
        
        # Rename the new table to the original name
        rename_table(cursor, "inputevents_clean", "inputevents")
        
        # Check the table schema using the reusable function
        check_table_schema(conn, "inputevents")
        
        # Commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")