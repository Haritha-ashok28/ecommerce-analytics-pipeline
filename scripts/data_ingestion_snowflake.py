# Load Olist CSV data files into Snowflake directly (no pandas)
import os
import re
import csv
import snowflake.connector
from dotenv import load_dotenv

def load_raw_data_to_snowflake(folder_path: str, schema: str = "RAW"):
    """
    Loads all Olist CSV files directly into Snowflake structured tables.
    - Automatically reads CSV headers to define columns.
    - Uses PUT + COPY INTO (no pandas).
    - Overwrites old tables safely.
    """
    load_dotenv()

    # --- Connect to Snowflake ---
    conn = snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database=os.getenv("SF_DATABASE"),
        schema=schema
    )
    cs = conn.cursor()

    # Create reusable CSV file format
    cs.execute("""
        CREATE OR REPLACE FILE FORMAT tmp_csv_format
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;
    """)
    print(" Temporary CSV file format created: tmp_csv_format")

    # Create temporary stage for uploads
    cs.execute("CREATE OR REPLACE TEMPORARY STAGE tmp_csv_stage")
    print(" Temporary stage created: tmp_csv_stage")

    # --- Iterate through all CSV files ---
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)
        table_name = re.sub(r"[^0-9A-Za-z_]+", "_", os.path.splitext(file_name)[0]).upper()

        print(f"\n Loading: {file_name} → {schema}.{table_name}")

        # --- Read header from CSV ---
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
        headers = [h.strip() for h in headers]

        # --- Create table definition dynamically ---
        col_defs = ", ".join([f'"{col}" STRING' for col in headers])
        create_sql = f'CREATE OR REPLACE TABLE {schema}."{table_name}" ({col_defs});'
        cs.execute(create_sql)
        print(f" Table created: {schema}.{table_name} ({len(headers)} columns)")

        # --- Upload CSV file to Snowflake stage ---
        put_cmd = f"PUT file://{file_path} @tmp_csv_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cs.execute(put_cmd)
        print(f" Uploaded {file_name} to Snowflake stage")

        # --- Copy data into Snowflake table ---
        copy_sql = f"""
            COPY INTO {schema}."{table_name}"
            FROM @tmp_csv_stage/{file_name}.gz
            FILE_FORMAT = (FORMAT_NAME = tmp_csv_format)
            ON_ERROR = 'CONTINUE';
        """
        cs.execute(copy_sql)
        print(f" Data loaded into {schema}.{table_name}")

    cs.close()
    conn.close()
    print("\n All CSV files loaded successfully into Snowflake!")

# --- Run script ---
if __name__ == "__main__":
    folder = r"d:\olist-business-analytics-pipeline\data\raw_data"
    load_raw_data_to_snowflake(folder, schema="RAW")

