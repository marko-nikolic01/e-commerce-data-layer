import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Load logs data into Hive")
      .enableHiveSupport()
      .getOrCreate()

    // HDFS path and Hive table
    val HDFS_NAMENODE = sys.env("CORE_CONF_fs_defaultFS")
    val LOGS_CSV_PATH = s"$HDFS_NAMENODE/data/raw/logs"
    val HIVE_TABLE = "logs"

    // Get current date and time using LocalDateTime
    val now = LocalDateTime.now()
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val filePath = s"$LOGS_CSV_PATH/${dateFormat.format(now)}"

    // Read CSV data
    var df = spark.read.option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(filePath)

    // Filter logs from the last 4 hours
    df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm"))

    val last4Hours = (1 to 4).map(i => (now.getHour - i + 24) % 24).toSet
    df = df.filter(hour(col("InvoiceDate")).isin(last4Hours.toSeq: _*))

    // Cast columns and drop duplicates
    df = df.withColumn("InvoiceDate", date_format(col("InvoiceDate"), "yyyy-MM-dd-HH-mm"))
    df = df.select(
      col("InvoiceNo").cast("string").alias("invoiceno"),
      col("StockCode").cast("string").alias("stockcode"),
      col("Quantity").cast("int").alias("quantity"),
      col("CustomerID").cast("string").alias("customerid"),
      col("Country").cast("string").alias("country"),
      col("InvoiceDate").cast("string").alias("invoicedate")
    ).dropDuplicates("invoiceno", "stockcode")

    // Create Hive tables if not exists
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $HIVE_TABLE (
        InvoiceNo STRING,
        StockCode STRING,
        Quantity INT,
        CustomerID STRING,
        Country STRING
      )
      PARTITIONED BY (InvoiceDate STRING)
      STORED AS PARQUET
    """)

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS unprocessed$HIVE_TABLE (
        InvoiceNo STRING,
        StockCode STRING,
        Quantity INT,
        CustomerID STRING,
        Country STRING,
        Invoicedate STRING
      )
      STORED AS PARQUET
    """)

    // Insert new rows
    if (df.count() > 0) {
      df.write.mode("append").insertInto(HIVE_TABLE)
    }

    // Stop Spark Session
    spark.stop()
  }
}
