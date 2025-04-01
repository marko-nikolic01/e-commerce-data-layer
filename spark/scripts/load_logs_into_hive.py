import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, date_format

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("Load logs data into Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS path and Hive table
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
LOGS_CSV_PATH = HDFS_NAMENODE + "/data/raw/logs"
HIVE_TABLE = "logs"

now = datetime.now()
file_path = f"{LOGS_CSV_PATH}/{now.year}/{now.month:02}/{now.day:02}"

# Read CSV data
df = spark.read.option("header", True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(file_path)

# Filter logs from last 4 hours
df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm"))

last_4_hours = [(now.hour - i) % 24 for i in range(1, 5)]
df = df.filter(hour(col("InvoiceDate")).isin(last_4_hours))

# Cast columns and drop duplicates
df = df.withColumn("InvoiceDate", date_format(col("InvoiceDate"), "yyyy-MM-dd-HH-mm"))
df = df.select(
    col("InvoiceNo").cast("int").alias("invoiceno"),
    col("StockCode").cast("int").alias("stockcode"),
    col("Quantity").cast("int").alias("quantity"),
    col("CustomerID").cast("int").alias("customerid"),
    col("Country").cast("string").alias("country"),
    col("InvoiceDate").cast("string").alias("invoicedate")
).dropDuplicates(["invoiceno", "stockcode"])

# Create Hive table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {HIVE_TABLE} (
        InvoiceNo INT,
        StockCode INT,
        Quantity INT,
        CustomerID INT,
        Country STRING
    )
    PARTITIONED BY (InvoiceDate STRING)
    STORED AS PARQUET
""")

if spark._jsparkSession.catalog().tableExists(HIVE_TABLE):
    existing_df = spark.table(HIVE_TABLE)

    print("\n\n\n")
    print(existing_df.count())
    print("\n\n\n")
    existing_df.show()

# Insert new rows
if df.count() > 0:
    df.write.mode("append").insertInto(HIVE_TABLE)

# Stop Spark Session
spark.stop()
