import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("Load countries data into Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS path and Hive table
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
COUNTRIES_CSV_PATH = HDFS_NAMENODE + "/data/raw/countries"
HIVE_TABLE = "countries"

# Read CSV data
df = spark.read.option("header", True).csv(COUNTRIES_CSV_PATH)

# Cast columns and drop duplicates
df = df.select(
    col("CountryID").cast("int"),
    col("CountryName").cast("string")
).dropDuplicates(["CountryID", "CountryName"])

# Create Hive table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {HIVE_TABLE} (
        CountryID INT,
        CountryName STRING
    )
    STORED AS PARQUET
""")

# Load existing Hive table into memory (if any)
if spark._jsparkSession.catalog().tableExists(HIVE_TABLE):
    existing_df = spark.table(HIVE_TABLE)
    
    # Filter out existing entries to avoid duplicates
    df = df.join(
        existing_df,
        on=["CountryID", "CountryName"],
        how="left_anti"
    )

# Insert new rows
if df.count() > 0:
    df.write.mode("append").insertInto(HIVE_TABLE)

# Stop Spark Session
spark.stop()
