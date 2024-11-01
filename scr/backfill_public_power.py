import requests
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType
from pyspark.sql import Row
from delta.tables import DeltaTable
import os
from datetime import datetime
import time
import argparse

# Import the countries dictionary from the config module
from countries_config import countries
# Import the Spark session initialization function
from initialize_spark_session import get_spark_session

# Parse command-line arguments to get the start date for backfilling
parser = argparse.ArgumentParser(description="Backfill energy data and save as Delta table.")
parser.add_argument('--start-date', type=str, required=True, help="Start date for backfilling (format: YYYY-MM-DD)")
args = parser.parse_args()

# Set the start and end dates for backfilling
start_date = args.start_date
end_date = datetime.now().strftime("%Y-%m-%d")

# Define the path for the Delta table in the user's home directory
home_dir = os.path.expanduser("~")
delta_path = os.path.join(home_dir, "public_power_table")

# Initialize Spark session
spark = get_spark_session()

# Define the schema for the DataFrame
schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("unix_seconds", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("production_type", StringType(), True),
    StructField("production_data", FloatType(), True),
    StructField("deprecated", BooleanType(), True)
])

# Initialize an empty list to collect rows for the DataFrame
all_data = []

# Loop through each country code and make an API request
for country_code, country_name in countries.items():
    url = f"https://api.energy-charts.info/public_power?country={country_code}&start={start_date}&end={end_date}"
    response = requests.get(url)
    
    if response.status_code == 200:
        json_data = response.json()
        
        # Extract unix_seconds and deprecated fields
        unix_seconds_list = json_data.get("unix_seconds", [])
        deprecated = json_data.get("deprecated", False)
        
        # Loop through each production type
        for production_type in json_data.get("production_types", []):
            production_name = production_type["name"]
            production_data_list = production_type["data"]
            
            # Pair unix_seconds with production_data
            for unix_seconds, production_data in zip(unix_seconds_list, production_data_list):
                timestamp = datetime.fromtimestamp(unix_seconds)
                row = Row(
                    country_code=country_code,
                    country_name=country_name,
                    unix_seconds=unix_seconds, 
                    timestamp=timestamp,
                    production_type=production_name, 
                    production_data=production_data, 
                    deprecated=deprecated
                )
                all_data.append(row)
    else:
        print(f"Failed to retrieve data for country {country_code}")

# Create DataFrame from the collected data
df = spark.createDataFrame(all_data, schema=schema)

# Save the DataFrame as a Delta table
df.write.format("delta").mode("append").save(delta_path)

# Confirm data is saved by loading the Delta table and showing its contents
delta_df = spark.read.format("delta").load(delta_path)
delta_df.show()
