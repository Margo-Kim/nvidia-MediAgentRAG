import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def drop_duplicate_in_prescriptions():
    # Connect to the SQLite database
    conn = sqlite3.connect('mimic.db')
    cursor = conn.cursor()

    # Query to find duplicate combinations of the primary key
    query_duplicates = """
    SELECT subject_id, hadm_id, pharmacy_id, poe_id, poe_seq, COUNT(*) AS duplicate_count
    FROM prescriptions
    GROUP BY subject_id, hadm_id, pharmacy_id, poe_id, poe_seq
    HAVING COUNT(*) > 1;
    """

    # Execute the query and load results into a DataFrame
    duplicates_df = pd.read_sql(query_duplicates, conn)

    print("Duplicate combinations of subject_id, hadm_id, pharmacy_id, poe_id, poe_seq:")
    print(duplicates_df)

    # Delete duplicate records, keeping one record per combination
    delete_duplicates_query = """
    DELETE FROM prescriptions
    WHERE ROWID NOT IN (
        SELECT MIN(ROWID)
        FROM prescriptions
        GROUP BY subject_id, hadm_id, pharmacy_id, poe_id, poe_seq
    );
    """

    # Execute the delete operation
    cursor.execute(delete_duplicates_query)
    conn.commit()

    print("Excess duplicate records have been deleted, retaining only one record per combination.")

    # Verify if there are still any duplicate combinations
    verify_query = """
    SELECT subject_id, hadm_id, pharmacy_id, poe_id, poe_seq, COUNT(*) AS duplicate_count
    FROM prescriptions
    GROUP BY subject_id, hadm_id, pharmacy_id, poe_id, poe_seq
    HAVING COUNT(*) > 1;
    """

    # Execute the verification query and load results into a DataFrame
    remaining_duplicates_df = pd.read_sql(verify_query, conn)

    print("Checking if any duplicate combinations remain:")
    print(remaining_duplicates_df)

    if remaining_duplicates_df.empty:
        print("All duplicate records have been successfully removed.")
    else:
        print("Some duplicate records still exist, please check the data.")

    # Close the database connection
    conn.close()
    print("Database connection closed.")

def clean_prescriptions():
    
    drop_duplicate_in_prescriptions()
    
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
        drop_table_query = "DROP TABLE IF EXISTS prescriptions_clean;"
        cursor.execute(drop_table_query)
        print(" 'prescriptions_clean' table dropped if existed. ")
        
        # create new table
        create_table_query = """
        CREATE TABLE prescriptions_clean (
            subject_id INTEGER,
            hadm_id INTEGER,
            pharmacy_id INTEGER,
            poe_id TEXT,
            poe_seq INTEGER,
            order_provider_id TEXT,
            starttime DATETIME,
            stoptime DATETIME,
            drug_type TEXT,
            drug TEXT,
            formulary_drug_cd TEXT,
            gsn TEXT,
            ndc TEXT,
            prod_strength TEXT,
            form_rx TEXT,
            dose_val_rx TEXT,
            dose_unit_rx TEXT,
            form_val_disp TEXT,
            form_unit_disp TEXT,
            doses_per_24_hrs INTEGER,
            route TEXT,
            PRIMARY KEY (hadm_id, pharmacy_id, poe_id, poe_seq)
        );
        """
        cursor.execute(create_table_query)
        print(" 'prescriptions_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO prescriptions_clean (subject_id,
            hadm_id,
            pharmacy_id,
            poe_id,
            poe_seq,
            order_provider_id,
            starttime,
            stoptime,
            drug_type,
            drug,
            formulary_drug_cd,
            gsn,
            ndc,
            prod_strength,
            form_rx,
            dose_val_rx,
            dose_unit_rx,
            form_val_disp,
            form_unit_disp,
            doses_per_24_hrs,
            route)
        SELECT subject_id,
            hadm_id,
            pharmacy_id,
            poe_id,
            poe_seq,
            order_provider_id,
            starttime,
            stoptime,
            drug_type,
            drug,
            formulary_drug_cd,
            gsn,
            ndc,
            prod_strength,
            form_rx,
            dose_val_rx,
            dose_unit_rx,
            form_val_disp,
            form_unit_disp,
            doses_per_24_hrs,
            route
        FROM prescriptions;
        """
        cursor.execute(copy_data_query)
        print(" 'prescriptions' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "prescriptions")
        
        # Rename the new table to the original name
        rename_table(cursor, "prescriptions_clean", "prescriptions")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "prescriptions")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")