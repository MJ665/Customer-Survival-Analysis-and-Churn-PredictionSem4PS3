import pymysql
from sqlalchemy import create_engine
import pandas as pd

# Database connection details
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234512345',
    'database': 'HMS100'
}

# Create a connection to the database using SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")

# Load data from the database tables
df_patient = pd.read_sql_query("SELECT * FROM Patient", engine)
df_patients_attend_appointments = pd.read_sql_query("SELECT * FROM PatientsAttendAppointments", engine)
df_patients_fill_history = pd.read_sql_query("SELECT * FROM PatientsFillHistory", engine)
df_diagnose = pd.read_sql_query("SELECT * FROM Diagnose", engine)
df_docs_have_schedules = pd.read_sql_query("SELECT * FROM DocsHaveSchedules", engine)
df_doctor_views_history = pd.read_sql_query("SELECT * FROM DoctorViewsHistory", engine)
# ...

# Merge dataframes based on foreign keys
merged_df = pd.merge(df_patient, df_patients_attend_appointments, left_on='email', right_on='patient', how='left')
merged_df = pd.merge(merged_df, df_patients_fill_history, left_on='email', right_on='patient', how='left')
merged_df = pd.merge(merged_df, df_diagnose, left_on='email', right_on='doctor', how='left')
merged_df = pd.merge(merged_df, df_docs_have_schedules, left_on='email', right_on='doctor', how='left')
merged_df = pd.merge(merged_df, df_doctor_views_history, left_on='email', right_on='doctor', how='left')

# Drop unnecessary columns
merged_df = merged_df.drop(['patient', 'doctor'], axis=1, errors='ignore')

# Define metadata for each column
metadata = {
    'email': {'data_type': 'VARCHAR(255)', 'description': 'Email address of the person'},
    'password': {'data_type': 'VARCHAR(255)', 'description': 'Password for authentication'},
    'name': {'data_type': 'VARCHAR(255)', 'description': 'Name of the person'},
    'address': {'data_type': 'VARCHAR(255)', 'description': 'Address of the person'},
    'gender': {'data_type': 'VARCHAR(255)', 'description': 'Gender of the person'},
    'patient_x': {'data_type': 'VARCHAR(255)', 'description': 'Patient email for appointment'},
    'appt_x': {'data_type': 'INT', 'description': 'Appointment count for patient_x'},
    # Add metadata for other columns
}

# Create a cursor
cursor = engine.connect().connection.cursor()

# Create a new table 'MergedData' in the database with metadata
create_table_query = f'''
CREATE TABLE MergedData (
    {', '.join([f'{column} {meta_info["data_type"]} COMMENT "{meta_info["description"]}"' for column, meta_info in metadata.items()])}
);
'''
cursor.execute(create_table_query)

# Store the merged_df in the 'MergedData' table using pandas.DataFrame.to_sql
merged_df.to_sql('MergedData', con=engine, if_exists='replace', index=False)

# Query the database to check the new table
result = pd.read_sql_query("SELECT * FROM MergedData", engine)

print(result)



