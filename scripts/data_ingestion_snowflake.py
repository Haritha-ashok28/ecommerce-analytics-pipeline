# Script to load raw CSV data files into Snowflake using write_pandas
import os
import re
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

def load_raw_data_to_snowflake(folder_path: str, schema: str = "RAW"):
    """
    Loads all raw CSVs from a folder into Snowflake tables using write_pandas.
    Each CSV = 1 table (filename -> tablename).
    """
    load_dotenv()

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database=os.getenv("SF_DATABASE"),
        schema=os.getenv("SF_SCHEMA")
    )
    cs = conn.cursor()
    #cs.execute(f"USE SCHEMA {schema}")

    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)
        table_name = re.sub(r"[^0-9A-Za-z_]+", "_", os.path.splitext(file_name)[0]).upper()
        print(f"\n Loading: {file_name} → {schema}.{table_name}")

        # Load and basic clean
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        df = df.drop_duplicates()

        # Infer datatypes
        dtype_map = {
            "object": "VARCHAR",
            "int64": "NUMBER",
            "float64": "FLOAT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP_NTZ",
        }

        # Attempt to parse date columns
        for col in df.columns:
            if "date" in col.lower() or "time" in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass

        col_defs = ", ".join(
            [f'"{col}" {dtype_map.get(str(dtype), "VARCHAR")}' for col, dtype in df.dtypes.items()]
        )

        # Create table
        create_sql = f'CREATE OR REPLACE TABLE "{table_name}" ({col_defs});'
        cs.execute(create_sql)
        print(f" Created table: {table_name}")

        # Upload via write_pandas
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
        print(f" Uploaded {nrows} rows to {schema}.{table_name}")

    cs.close()
    conn.close()
    print("\n All files uploaded successfully!")

# Run it
folder = r"d:\olist-business-analytics-pipeline\data\raw_data"
load_raw_data_to_snowflake(folder, schema="RAW")
