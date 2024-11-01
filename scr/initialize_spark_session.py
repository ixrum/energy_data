from pyspark.sql import SparkSession

def get_spark_session(app_name="EnergyData"):
    """
    Create and configure a SparkSession.

    Args:
        app_name (str): The name of the Spark application. Default is "EnergyData".

    Returns:
        SparkSession: A configured SparkSession instance.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-client:3.3.1")
        .config("spark.master", "local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.hadoop.security.authentication", "simple")
        .config("spark.hadoop.security.authorization", "false")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )
