import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session with Hive support
    val spark = SparkSession.builder
      .appName("Load countries data into Hive")
      .enableHiveSupport()
      .getOrCreate()

    // HDFS path and Hive table
    val HDFS_NAMENODE = sys.env("CORE_CONF_fs_defaultFS")
    val COUNTRIES_CSV_PATH = s"$HDFS_NAMENODE/data/raw/countries"
    val HIVE_TABLE = "countries"

    // Read CSV data
    var df: DataFrame = spark.read.option("header", "true").csv(COUNTRIES_CSV_PATH)

    // Cast columns and drop duplicates
    df = df.select(
      col("CountryID").cast("int").alias("countryid"),
      col("CountryName").cast("string").alias("countryname")
    ).dropDuplicates("countryid", "countryname")

    // Create Hive table if not exists
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $HIVE_TABLE (
        countryid INT,
        countryname STRING
      )
      STORED AS PARQUET
    """)

    // Load existing Hive data
    if (spark.catalog.tableExists(HIVE_TABLE)) {
      val existing_df = spark.table(HIVE_TABLE)

      // Filter out duplicates
      df = df.join(
        existing_df,
        Seq("countryid", "countryname"),
        "left_anti"
      )
    }

    // Insert new rows
    df = df.filter(col("countryid").isNotNull)
    if (df.count() > 0) {
      df.write.mode("append").insertInto(HIVE_TABLE)
    }

    // Stop Spark Session
    spark.stop()
  }
}
