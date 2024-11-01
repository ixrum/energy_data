import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os
from datetime import datetime, timedelta

# Import get_spark_session from the initialize_spark_session module
from initialize_spark_session import get_spark_session
# Import bzn list from the bzn_list module
from bzn_list import bzn

def get_price_data(bzn_list, data_date, spark):
    """
    Fetch power data from the given API URL and return it as a Spark DataFrame.

    Parameters:
        bzn_list (list): List of bidding zones.
        data_date (str): Date for which data needs to be fetched (YYYY-MM-DD).
        spark (SparkSession): Active Spark session.

    Returns:
        DataFrame: Spark DataFrame containing the power price data.
    """
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("bzn", StringType(), True),
        StructField("price", DoubleType(), True)
    ])
    
    records = []
    
    for each_bzn in bzn_list:
        api_url = f"https://api.energy-charts.info/price?bzn={each_bzn}&start={data_date}&end={data_date}"

        try:
            response = requests.get(api_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for bzn ({each_bzn}): {e}")
            continue
        
        data = response.json()
        
        # Extract relevant data for DataFrame creation
        if isinstance(data, dict) and "unix_seconds" in data and "price" in data:
            unix_seconds = data["unix_seconds"]
            price = data["price"]

            if len(price) == len(unix_seconds):
                for timestamp, price_eur_mwh in zip(unix_seconds, price):
                    records.append((
                        datetime.utcfromtimestamp(timestamp).isoformat(),
                        each_bzn,
                        price_eur_mwh
                    ))
            else:
                print(f"Data length mismatch for timestamp and price for bzn {each_bzn}")
        else:
            print(f"Unexpected data format from API for bzn ({each_bzn}).")
            continue
    
    # Create a DataFrame from the extracted records
    df = spark.createDataFrame(data=records, schema=schema)
    
    return df

def main():
    data_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Get Spark session
    spark = get_spark_session()
    
    # Extracting data from API
    price_data_df = get_price_data(bzn, data_date, spark)

    if not price_data_df.rdd.isEmpty():
        try:
            delta_table_path = os.path.expanduser("~/price_data")
            
            if not os.path.exists(delta_table_path):
                os.makedirs(delta_table_path)
            
            price_data_df.write.format("delta").mode("overwrite").save(delta_table_path)
            print(f"Data loaded into Delta Table at {delta_table_path}")
        except Exception as e:
            print(f"Error while writing to Delta Table: {e}")
        finally:
            spark.stop()
    else:
        print("No data found.")

if __name__ == "__main__":
    main()
