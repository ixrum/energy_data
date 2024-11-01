import os
import requests
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType
from pyspark.sql import Row
from delta.tables import DeltaTable
from initialize_spark_session import get_spark_session
from countries_config import countries

# Initialize Spark session using external file
spark = get_spark_session()

# Define the schema for the installed power DataFrame
installed_power_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("time", StringType(), True),
    StructField("production_type", StringType(), True),
    StructField("installed_power_gw", FloatType(), True),
    StructField("deprecated", BooleanType(), True)
])

# Initialize an empty list to collect rows for the DataFrame
installed_power_data = []

# Loop through each setting for installation_decommission
for decommission in ["false", "true"]:
    # Loop through each country code and make an API request
    for country_code, country_name in countries.items():
        # Set the parameters for the API request
        url = "https://api.energy-charts.info/installed_power"
        params = {
            "country": country_code,
            "time_step": "yearly",
            "installation_decommission": decommission
        }

        try:
            # Make the API request
            response = requests.get(url, params=params)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to retrieve installed power data for country {country_code} with decommission={decommission}: {e}")
            continue

        json_data = response.json()

        # Extract time and deprecated fields
        time_list = json_data.get("time", [])
        deprecated = json_data.get("deprecated", False)

        # Loop through each production type
        for production_type in json_data.get("production_types", []):
            production_name = production_type["name"]
            production_data_list = production_type["data"]

            # Pair time with production data
            for time, installed_power in zip(time_list, production_data_list):
                row = Row(
                    country_code=country_code,
                    country_name=country_name,
                    time=time,
                    production_type=production_name,
                    installed_power_gw=installed_power,
                    deprecated=deprecated
                )
                installed_power_data.append(row)

# Create DataFrame from the collected data
installed_power_df = spark.createDataFrame(installed_power_data, schema=installed_power_schema)

# Define the path for the Delta table
installed_power_delta_path = os.path.expanduser("~/installed_power")

# Save the DataFrame as a Delta table
installed_power_df.write.format("delta").mode("overwrite").save(installed_power_delta_path)

# Confirm data is saved by loading the Delta table and showing its contents
installed_power_delta_df = spark.read.format("delta").load(installed_power_delta_path)
installed_power_delta_df.show()
