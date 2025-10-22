# Script to preprocess raw CSV data files: load, inspect, clean, and save cleaned versions
import pandas as pd
import os

# Define paths
raw_data_path = r'd:\olist-business-analytics-pipeline\data\raw_data'
processed_data_path = r'd:\olist-business-analytics-pipeline\data\cleaned_data'

# List all CSV files in the raw data directory
csv_files = [i for i in os.listdir(raw_data_path) if i.endswith('.csv')]

# Create the cleaned_data directory if it doesn't exist
if not os.path.exists(processed_data_path):
    os.makedirs(processed_data_path)

# Load, inspect, clean, and save each CSV file
for file_name in csv_files:
    print(f"\n Processing file: {file_name}")
    
    raw_file_path = os.path.join(raw_data_path, file_name)
    df = pd.read_csv(raw_file_path)

    # --- EDA: Print info and missing values ---
    print(" Data Info:")
    print(df.info())

    print("\n Missing Values per Column:")
    print(df.isnull().sum())

    # --- Basic Cleaning ---
    df.columns = df.columns.str.strip()  # Remove leading/trailing spaces from column names
    df = df.drop_duplicates()  # Drop duplicate rows

    # # Optional: Drop rows with all NaN (empty rows)
    # df = df.dropna(how='all')

    # # Optional: You could fill missing values if needed:
    # # df = df.fillna(method='ffill')  # forward-fill as example

    # --- Save Cleaned Data ---
    processed_file_path = os.path.join(processed_data_path, file_name)
    df.to_csv(processed_file_path, index=False)

    print(f" Cleaned and saved: {processed_file_path} | Shape: {df.shape}")

print("\n All files processed and saved.")
