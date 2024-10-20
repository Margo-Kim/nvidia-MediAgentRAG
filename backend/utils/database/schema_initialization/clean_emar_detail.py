import pandas as pd
import sqlite3
import glob
import os

from schema_initialization.data_utils import *
def clean_emar_detail():
    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, '..', 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        # Connect to the database using the relative path
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Drop the old table if it exists
        drop_table_query = "DROP TABLE IF EXISTS emar_detail_clean;"
        cursor.execute(drop_table_query)
        print(" 'emar_detail_clean' table dropped if existed. ")
        
        # create new table
        create_table_query = """
        CREATE TABLE emar_detail_clean (
            subject_id INTEGER,
            emar_id TEXT,
            emar_seq INTEGER,
            parent_field_ordinal TEXT,
            administration_type TEXT,
            pharmacy_id INTEGER,
            barcode_type TEXT,
            reason_for_no_barcode TEXT,
            complete_dose_not_given TEXT,
            dose_due TEXT,
            dose_due_unit TEXT,
            dose_given TEXT,
            dose_given_unit TEXT,
            will_remainder_of_dose_be_given TEXT,
            product_amount_given TEXT,
            product_unit TEXT,
            product_code TEXT,
            product_description TEXT,
            product_description_other TEXT,
            prior_infusion_rate TEXT,
            infusion_rate TEXT,
            infusion_rate_adjustment TEXT,
            infusion_rate_adjustment_amount TEXT,
            infusion_rate_unit TEXT,
            route TEXT,
            infusion_complete TEXT,
            completion_interval TEXT,
            new_iv_bag_hung TEXT,
            continued_infusion_in_other_location TEXT,
            restart_interval TEXT,
            side TEXT,
            site TEXT,
            non_formulary_visual_verification TEXT,
            PRIMARY KEY (emar_id, parent_field_ordinal)
        );
        """
        cursor.execute(create_table_query)
        print(" 'emar_detail_clean' is successfully created. ")
        
        # copy the data from the old table to the new one
        copy_data_query = """
        INSERT INTO emar_detail_clean (subject_id,
            emar_id,
            emar_seq,
            parent_field_ordinal,
            administration_type,
            pharmacy_id,
            barcode_type,
            reason_for_no_barcode,
            complete_dose_not_given,
            dose_due,
            dose_due_unit,
            dose_given,
            dose_given_unit,
            will_remainder_of_dose_be_given,
            product_amount_given,
            product_unit,
            product_code,
            product_description,
            product_description_other,
            prior_infusion_rate,
            infusion_rate,
            infusion_rate_adjustment,
            infusion_rate_adjustment_amount,
            infusion_rate_unit,
            route,
            infusion_complete,
            completion_interval,
            new_iv_bag_hung,
            continued_infusion_in_other_location,
            restart_interval,
            side,
            site,
            non_formulary_visual_verification)
        SELECT subject_id,
            emar_id,
            emar_seq,
            parent_field_ordinal,
            administration_type,
            pharmacy_id,
            barcode_type,
            reason_for_no_barcode,
            complete_dose_not_given,
            dose_due,
            dose_due_unit,
            dose_given,
            dose_given_unit,
            will_remainder_of_dose_be_given,
            product_amount_given,
            product_unit,
            product_code,
            product_description,
            product_description_other,
            prior_infusion_rate,
            infusion_rate,
            infusion_rate_adjustment,
            infusion_rate_adjustment_amount,
            infusion_rate_unit,
            route,
            infusion_complete,
            completion_interval,
            new_iv_bag_hung,
            continued_infusion_in_other_location,
            restart_interval,
            side,
            site,
            non_formulary_visual_verification
        FROM emar_detail;
        """
        cursor.execute(copy_data_query)
        print(" 'emar_detail' is successfully copied. ")

        # Drop the old table using the reusable function
        drop_table(cursor, "emar_detail")
        
        # Rename the new table to the original name
        rename_table(cursor, "emar_detail_clean", "emar_detail")
        
        # Use the new function to check the table schema
        check_table_schema(conn, "emar_detail")
        
        # commit the changes
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # close the connection
        if conn:
            conn.close()
            print("Database connection closed.")