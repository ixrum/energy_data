import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from initialize_spark_session import get_spark_session

# Initialize Spark session
spark = get_spark_session(app_name="DeltaTableExample")

# Construct the path to the Delta table in the user's home directory
input_delta_path = os.path.join(os.path.expanduser("~"), "public_power_table")

# Read data from Delta table
input_df = spark.read.format("delta").load(input_delta_path)

# Generate unique IDs for each production type
# Step 1: Select distinct production types
distinct_production_df = input_df.select("production_type").distinct()

# Step 2: Generate unique IDs for each production type using row_number()
window_spec = Window.orderBy("production_type")
production_type_id_df = distinct_production_df \
    .withColumn("production_type_id", F.row_number().over(window_spec))

# Construct the path to save the Delta table in the user's home directory
output_delta_path = os.path.join(os.path.expanduser("~"), "dim_production_type")

# Save the DataFrame as a Delta table
production_type_id_df.write.format("delta").mode("overwrite").save(output_delta_path)
