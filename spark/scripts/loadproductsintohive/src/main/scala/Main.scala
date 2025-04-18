import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Date

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session with Hive support
    val spark = SparkSession.builder()
      .appName("Load product data into Hive")
      .enableHiveSupport()
      .getOrCreate()

    // HDFS path and Hive table
    val HDFS_NAMENODE = sys.env("CORE_CONF_fs_defaultFS")
    val PRODUCTS_CSV_PATH = s"$HDFS_NAMENODE/data/raw/products"
    val HIVE_TABLE = "products"

    // Get current date
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy/MM/dd")
    val filePath = s"$PRODUCTS_CSV_PATH/${dateFormat.format(now)}"

    // Read CSV data
    var df: DataFrame = spark.read.option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(filePath)

    // Cast columns and drop duplicates
    df = df.select(
      col("StockCode").cast("string").alias("stockcode"),
      col("ProductName").cast("string").alias("productname"),
      col("ProductDescription").cast("string").alias("productdescription"),
      col("UnitPrice").cast("decimal(10,5)").alias("unitprice"),
      col("Date").cast("string").alias("date")
    ).dropDuplicates("stockcode", "date")

    // Create Hive table if not exists
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $HIVE_TABLE (
        StockCode STRING,
        ProductName STRING,
        ProductDescription STRING,
        UnitPrice DECIMAL(10,5)
      )
      PARTITIONED BY (Date STRING)
      STORED AS PARQUET
    """)

    // Insert new rows if there are any
    if (df.count() > 0) {
      df.write.mode("append").insertInto(HIVE_TABLE)
    }

    // Stop Spark Session
    spark.stop()
  }
}
