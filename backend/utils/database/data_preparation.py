import sys
import os

from data_import import *

from schema_initialization.clean_admissions import clean_admissions
from schema_initialization.clean_caregiver import clean_caregiver
from schema_initialization.clean_chartevents import clean_chartevents
from schema_initialization.clean_d_hcpcs import clean_d_hcpcs
from schema_initialization.clean_d_icd_diagnoses import clean_d_icd_diagnoses
from schema_initialization.clean_d_icd_procedures import clean_d_icd_procedures
from schema_initialization.clean_d_items import clean_d_items
from schema_initialization.clean_d_labitems import clean_d_labitems
from schema_initialization.clean_datetimeevents import clean_datetimeevents
from schema_initialization.clean_diagnoses_icd import clean_diagnoses_icd
from schema_initialization.clean_drgcodes import clean_drgcodes
from schema_initialization.clean_emar_detail import clean_emar_detail
from schema_initialization.clean_emar import clean_emar
from schema_initialization.clean_hcpcsevents import clean_hcpcsevents
from schema_initialization.clean_icustays import clean_icustays
from schema_initialization.clean_ingredientevents import clean_ingredientevents
from schema_initialization.clean_inputevents import clean_inputevents
from schema_initialization.clean_labevents import clean_labevents
from schema_initialization.clean_microbiologyevents import clean_microbiologyevents
from schema_initialization.clean_omr import clean_omr
from schema_initialization.clean_outputevents import clean_outputevents
from schema_initialization.clean_patients import clean_patients
from schema_initialization.clean_pharmacy import clean_pharmacy
from schema_initialization.clean_poe_detail import clean_poe_detail
from schema_initialization.clean_poe import clean_poe
from schema_initialization.clean_prescriptions import clean_prescriptions
from schema_initialization.clean_procedureevents import clean_procedureevents
from schema_initialization.clean_procedures_icd import clean_procedures_icd
from schema_initialization.clean_provider import clean_provider
from schema_initialization.clean_services import clean_services
from schema_initialization.clean_transfers import clean_transfers

from index_creation import *

# Define a function to call all the cleaning functions
def call_cleaning_functions():
    # List of all cleaning function names (make sure the functions are correctly imported)
    cleaning_functions = [
        clean_admissions,
        clean_caregiver,
        clean_chartevents,
        clean_d_hcpcs,
        clean_d_icd_diagnoses,
        clean_d_icd_procedures,
        clean_d_items,
        clean_d_labitems,
        clean_datetimeevents,
        clean_diagnoses_icd,
        clean_drgcodes,
        clean_emar_detail,
        clean_emar,
        clean_hcpcsevents,
        clean_icustays,
        clean_ingredientevents,
        clean_inputevents,
        clean_labevents,
        clean_microbiologyevents,
        clean_omr,
        clean_outputevents,
        clean_patients,
        clean_pharmacy,
        clean_poe_detail,
        clean_poe,
        clean_prescriptions,
        clean_procedureevents,
        clean_procedures_icd,
        clean_provider,
        clean_services,
        clean_transfers
    ]

    # Loop through each cleaning function and call it
    for cleaning_function in cleaning_functions:
        try:
            # Get the function name from the function object itself
            function_name = cleaning_function.__name__
            
            # Print which function is being run
            print(f"Running {function_name}...")
            
            # Call the cleaning function
            cleaning_function()  # Assuming each function takes no arguments
            
            print(f"{function_name} completed successfully.\n")
        except Exception as e:
            print(f"Error occurred while running {function_name}: {e}")

if __name__ == "__main__":
    # Import Data
    data_import()

    # Call all cleaning functions
    call_cleaning_functions()

    # Create Index to accelarate
    create_index_all()