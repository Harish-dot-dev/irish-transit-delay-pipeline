import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

conn = snowflake.connector.connect(
    account   = os.getenv("SNOWFLAKE_ACCOUNT"),
    user      = os.getenv("SNOWFLAKE_USER"),
    password  = os.getenv("SNOWFLAKE_PASSWORD"),
    database  = os.getenv("SNOWFLAKE_DATABASE"),
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
    schema    = os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
row = cursor.fetchone()
print(f"Connected as : {row[0]}")
print(f"Database     : {row[1]}")
print(f"Warehouse    : {row[2]}")
cursor.close()
conn.close()
print("Connection successful!")