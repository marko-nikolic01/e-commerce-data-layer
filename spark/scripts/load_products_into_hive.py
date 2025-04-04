import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("Load product data into Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS path and Hive table
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
PRODUCTS_CSV_PATH = HDFS_NAMENODE + "/data/raw/products"
HIVE_TABLE = "products"

now = datetime.now()
file_path = f"{PRODUCTS_CSV_PATH}/{now.year}/{now.month:02}/{now.day:02}"

# Read CSV data
df = spark.read.option("header", True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(file_path)

# Cast columns adn drop duplicates
df = df.select(
    col("StockCode").cast("string").alias("stockcode"),
    col("ProductName").cast("string").alias("productname"),
    col("ProductDescription").cast("string").alias("productdescription"),
    col("UnitPrice").cast("decimal(10,5)").alias("unitprice"),
    col("Date").cast("string").alias("date")
).dropDuplicates(["stockcode", "date"])

# Create Hive table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {HIVE_TABLE} (
        StockCode STRING,
        ProductName STRING,
        ProductDescription STRING,
        UnitPrice DECIMAL(10,5)
    )
    PARTITIONED BY (Date STRING)
    STORED AS PARQUET
""")

# Insert new rows
if df.count() > 0:
    df.write.mode("append").insertInto(HIVE_TABLE)

# Stop Spark Session
spark.stop()
