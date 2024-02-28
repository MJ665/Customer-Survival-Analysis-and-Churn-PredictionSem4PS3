import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine

# Connect to MySQL database
db_connection_str = 'mysql+pymysql://root:1234512345@localhost:3306/ps3  # Update with your database credentials
engine = create_engine(db_connection_str)

# Read data from the Customer table into a Pandas DataFrame
sql_query = "SELECT * FROM Customer;"
customer_data = pd.read_sql(sql_query, con=engine)

# Display the original data
print("Original Data:")
print(customer_data)

# Detect missing values
missing_values = customer_data.isnull()

# Handling missing values
# For simplicity, let's fill missing numeric values with the mean and missing categorical values with the mode
customer_data['age'].fillna(customer_data['age'].mean(), inplace=True)

# Check if there are non-null values in the 'gender' column before filling missing values
if not customer_data['gender'].notnull().all():
    customer_data['gender'].fillna(customer_data['gender'].mode()[0], inplace=True)

customer_data['address'].fillna('Unknown', inplace=True)
customer_data['purchase_history'].fillna('No Purchases', inplace=True)

# Display the data after handling missing values
print("\nData after handling missing values:")
print(customer_data)

# Statistical operations
# Display summary statistics
summary_statistics = customer_data.describe()

# Display the count of missing values for each column
missing_values_count = customer_data.isnull().sum()

# Print results
print("\nSummary Statistics:")
print(summary_statistics)
print("\nCount of Missing Values:")
print(missing_values_count)
