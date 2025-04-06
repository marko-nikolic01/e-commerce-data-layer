import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, date_format

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("Load unprocessed logs data into Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS path and Hive table
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
LOGS_PARQUET_PATH = HDFS_NAMENODE + "/data/temp/logs"
HIVE_TABLE = "unprocessedlogs"

# Read CSV data
logs = spark.read.parquet(LOGS_PARQUET_PATH)

spark.sql(f"""
    DROP TABLE IF EXISTS {HIVE_TABLE}
""")

# Reset Hive table
spark.sql(f"""
    DROP TABLE IF EXISTS {HIVE_TABLE}
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {HIVE_TABLE} (
        InvoiceNo INT,
        StockCode INT,
        Quantity INT,
        CustomerID INT,
        Country STRING,
        InvoiceDate STRING
    )
    STORED AS PARQUET
""")

# Insert new rows
logs.write.mode("append").insertInto(HIVE_TABLE)

# Stop Spark Session
spark.stop()
