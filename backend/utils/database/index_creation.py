import pandas as pd
import sqlite3
import glob
import os

def create_index(table_name, index_name, attributes):
    """
    Creates an index on specified attributes of a given table.
    """
    try:
        # Get the path of the current script (where the Python script is located)
        script_dir = os.path.dirname(__file__)

        # Construct the relative path to the mimic.db file
        db_path = os.path.join(script_dir, 'mimic.db')  # Go up one directory and look for mimic.db in 'database' folder

        with sqlite3.connect('mimic.db') as conn:
            cursor = conn.cursor()

            # Drop the index if it already exists
            drop_index_query = f"DROP INDEX IF EXISTS {index_name};"
            cursor.execute(drop_index_query)

            # Prepare the SQL command to create an index
            attributes_str = ", ".join(attributes)
            create_index_query = f"CREATE INDEX {index_name} ON {table_name}({attributes_str});"

            # Execute the query
            cursor.execute(create_index_query)
            print(f"Index '{index_name}' created successfully on {attributes_str} in '{table_name}' table.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

def create_index_all():
    # Create an index on the subject_id column in the admissions table
    create_index("admissions", "idx_subject_id_admissions", ["subject_id"])
    
    # Create an index on the subject_id column in the transfers table
    create_index("transfers", "idx_subject_id_transfers", ["subject_id"])
    
    # Create a composite index on subject_id and hadm_id in the transfers table
    create_index("transfers", "idx_subject_id_hadm_id_transfers", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the hcpcsevents table
    create_index("hcpcsevents", "idx_subject_id_hadm_id_hcpcsevents", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the labevents table
    create_index("labevents", "idx_subject_id_hadm_id_labevents", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the microbiologyevents table
    create_index("microbiologyevents", "idx_subject_id_hadm_id_microbiologyevents", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the poe table
    create_index("poe", "idx_subject_id_hadm_id_poe", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the emar table
    create_index("emar", "idx_subject_id_hadm_id_emar", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the prescriptions table
    create_index("prescriptions", "idx_subject_id_hadm_id_prescriptions", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the pharmacy table
    create_index("pharmacy", "idx_subject_id_hadm_id_pharmacy", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the diagnoses_icd table
    create_index("diagnoses_icd", "idx_subject_id_hadm_id_diagnoses_icd", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the procedures_icd table
    create_index("procedures_icd", "idx_subject_id_hadm_id_procedures_icd", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the drgcodes table
    create_index("drgcodes", "idx_subject_id_hadm_id_drgcodes", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the services table
    create_index("services", "idx_subject_id_hadm_id_services", ["subject_id", "hadm_id"])
    
    # Create an index on hadm_id in the services table for faster lookups on procedures
    create_index("services", "idx_hadm_id_procedures_services", ["hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the icustays table
    create_index("icustays", "idx_subject_id_hadm_id_icustays", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the inputevents table
    create_index("inputevents", "idx_subject_id_hadm_id_inputevents", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the ingredientevents table
    create_index("ingredientevents", "idx_subject_id_hadm_id_ingredientevents", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the outputevents table
    create_index("outputevents", "idx_subject_id_hadm_id_outputevents", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the procedureevents table
    create_index("procedureevents", "idx_subject_id_hadm_id_procedureevents", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the datetimeevents table
    create_index("datetimeevents", "idx_subject_id_hadm_id_datetimeevents", ["subject_id", "hadm_id"])
    
    # Create a composite index on subject_id and hadm_id in the chartevents table
    create_index("chartevents", "idx_subject_id_hadm_id_chartevents", ["subject_id", "hadm_id"])
    
    # Create a composite index on emar_id and emar_seq in the emar_detail table
    create_index("emar_detail", "idx_emar_id_emar_seq_emar_detail", ["emar_id", "emar_seq"])
