def fetch_admission_data(cursor, subject_id):
    """Fetch admissions and patients data for the given subject_id."""
    query = """
    SELECT admissions.hadm_id, admissions.admittime, admissions.dischtime, admissions.admission_type, 
           admissions.insurance, admissions.marital_status, 
           patients.subject_id, patients.gender, patients.anchor_age
    FROM admissions
    JOIN patients ON admissions.subject_id = patients.subject_id
    WHERE admissions.subject_id = ?
    """
    # Execute the query to retrieve admissions and patients data for the specified subject_id
    cursor.execute(query, (subject_id,))
    
    # Return all the fetched rows
    return cursor.fetchall()

def fetch_transfer_data(cursor, hadm_id):
    """Fetch transfer data including label for the given hadm_id."""
    query = """
    SELECT transfers.transfer_id, transfers.eventtype, transfers.careunit, 
           transfers.intime, transfers.outtime
    FROM transfers
    WHERE transfers.hadm_id = ?
    """
    # Execute the query to retrieve transfer data for the specified hadm_id
    cursor.execute(query, (hadm_id,))
    
    # Return the results sorted by intime (column index 3)
    return sorted(cursor.fetchall(), key=lambda x: x[3])

def fetch_services_data(cursor, hadm_id):
    """Fetch services data for the given hadm_id."""
    query = """
    SELECT services.transfertime, services.prev_service, services.curr_service
    FROM services
    WHERE services.hadm_id = ?
    """
    # Execute the query to retrieve services data for the specified hadm_id
    cursor.execute(query, (hadm_id,))
    
    # Return the results sorted by transfertime (column index 0)
    return sorted(cursor.fetchall(), key=lambda x: x[0])

def fetch_procedures_data(cursor, hadm_id):
    """Fetch procedures data for the given hadm_id."""
    query = """
    SELECT procedures_icd.chartdate, d_icd_procedures.long_title
    FROM procedures_icd
    LEFT JOIN d_icd_procedures 
    ON procedures_icd.icd_code = d_icd_procedures.icd_code 
    AND procedures_icd.icd_version = d_icd_procedures.icd_version
    WHERE procedures_icd.hadm_id = ?
    """
    # Execute the query to retrieve procedures data for the specified hadm_id
    cursor.execute(query, (hadm_id,))
    
    # Return the results sorted by chartdate (column index 0)
    return sorted(cursor.fetchall(), key=lambda x: x[0])

def fetch_prescriptions_data(cursor, hadm_id):
    """Fetch prescriptions data for the given hadm_id."""
    query = """
    SELECT prescriptions.starttime, prescriptions.stoptime, prescriptions.drug_type,
           prescriptions.drug, prescriptions.prod_strength, prescriptions.form_rx,
           prescriptions.dose_val_rx, prescriptions.dose_unit_rx, prescriptions.form_val_disp,
           prescriptions.form_unit_disp, prescriptions.doses_per_24_hrs, prescriptions.route
    FROM prescriptions
    WHERE prescriptions.hadm_id = ?
    """
    # Execute the query to retrieve prescriptions data for the specified hadm_id
    cursor.execute(query, (hadm_id,))
    
    # Return all fetched results
    return cursor.fetchall()

def fetch_icustays_data(cursor, hadm_id):
    """Fetch icustays data for the given hadm_id."""
    query = """
    SELECT stay_id, first_careunit, last_careunit, intime, outtime, los
    FROM icustays
    WHERE hadm_id = ?
    """
    # Execute the query to retrieve icustays data for the specified hadm_id
    cursor.execute(query, (hadm_id,))
    
    # Return the results sorted by intime (column index 3)
    return sorted(cursor.fetchall(), key=lambda x: x[3])

def fetch_datetimeevents_data(cursor, stay_id):
    """Fetch datetimeevents data for the given stay_id."""
    query = """
    SELECT dte.charttime, dte.value, di.label, di.abbreviation, di.category, di.unitname
    FROM datetimeevents dte
    JOIN d_items di ON dte.itemid = di.itemid
    WHERE dte.stay_id = ?
    """
    # Execute the query to retrieve datetimeevents data for the specified stay_id
    cursor.execute(query, (stay_id,))
    
    # Return the results sorted by charttime (column index 0)
    return sorted(cursor.fetchall(), key=lambda x: x[0])

def fetch_inputevents_data(cursor, stay_id):
    """Fetch inputevents data for the given stay_id."""
    query = """
    SELECT ie.starttime, ie.endtime, ie.amount, ie.amountuom, ie.rate, ie.rateuom, 
           ie.patientweight, di.label, di.category, di.unitname
    FROM inputevents ie
    JOIN d_items di ON ie.itemid = di.itemid
    WHERE ie.stay_id = ?
    """
    # Execute the query to retrieve inputevents data for the specified stay_id
    cursor.execute(query, (stay_id,))
    
    # Return the results sorted by starttime (column index 0)
    return sorted(cursor.fetchall(), key=lambda x: x[0])

def fetch_ingredientevents_data(cursor, stay_id):
    """Fetch ingredientevents data for the given stay_id."""
    query = """
    SELECT ie.starttime, ie.endtime, ie.amount, ie.amountuom, ie.rate, ie.rateuom, 
           di.label, di.unitname
    FROM ingredientevents AS ie
    LEFT JOIN d_items AS di ON ie.itemid = di.itemid
    WHERE ie.stay_id = ?
    """
    # Execute the query to retrieve ingredientevents data for the specified stay_id
    cursor.execute(query, (stay_id,))
    
    # Return the results sorted by starttime (column index 0)
    return sorted(cursor.fetchall(), key=lambda x: x[0])

def fetch_outputevents_data(cursor, stay_id):
    """Fetch outputevents data for the given stay_id."""
    query = """
    SELECT oe.charttime, oe.value, di.label, di.unitname
    FROM outputevents AS oe
    LEFT JOIN d_items AS di ON oe.itemid = di.itemid
    WHERE oe.stay_id = ?
    """
    # Execute the query to retrieve outputevents data for the specified stay_id
    cursor.execute(query, (stay_id,))
    
    # Return the results sorted by charttime (column index 0)
    return sorted(cursor.fetchall(), key=lambda x: x[0])

def fetch_procedureevents_data(cursor, stay_id):
    """Fetch procedureevents data for the given stay_id."""
    query = """
    SELECT pe.starttime, pe.endtime, pe.value, pe.valueuom, pe.location, pe.locationcategory, 
           pe.patientweight, di.label, di.abbreviation, di.category, di.unitname
    FROM procedureevents AS pe
    LEFT JOIN d_items AS di ON pe.itemid = di.itemid
    WHERE pe.stay_id = ?
    """
    # Execute the query to retrieve procedureevents data for the specified stay_id
    cursor.execute(query, (stay_id,))
    
    # Return the results sorted by starttime (column index 0)
    return sorted(cursor.fetchall(), key=lambda x: x[0])

def fetch_chartevents_data(cursor, stay_id):
    """Fetch chartevents data for the given stay_id."""
    query = """
    SELECT ce.charttime, ce.value, ce.valueuom, di.label, di.category, di.unitname
    FROM chartevents AS ce
    LEFT JOIN d_items AS di ON ce.itemid = di.itemid
    WHERE ce.stay_id = ?
    """
    # Execute the query to retrieve chartevents data for the specified stay_id
    cursor.execute(query, (stay_id,))
    
    # Return the results sorted by charttime (column index 0)
    return sorted(cursor.fetchall(), key=lambda x: x[0])
