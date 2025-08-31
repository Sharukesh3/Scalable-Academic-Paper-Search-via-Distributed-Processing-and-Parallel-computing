import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * A Spark application to read CORE JSON metadata, clean it, and write it to Parquet format.
 */
object CoreDataCleaner {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CORE Data Cleaner")
      // This master URL is for running on the cluster via spark-submit.
      // For local testing, you would change it to .master("local[*]")
      .master("spark://asaicomputenode03.amritanet.edu:7077")
      .getOrCreate()

    // Import spark implicits for '$' syntax
    import spark.implicits._

    // --- 1. Read, Select, and Clean Data ---
    // This is a single, chained transformation for maximum efficiency.
    // Spark's optimizer will push down the selections and filters.
    val cleanedDF = spark.read
      // Reads each line as a separate JSON object (correct for your data)
      .json("hdfs:///s2orc_paper_data_extracted/")
      // Filter out rows where either 'title' or 'abstract' is null
      .na.drop(Seq("title"))


    // --- 2. Write Cleaned Data to Parquet ---
    cleanedDF
      // Consolidate data into a reasonable number of output files (adjust as needed)
      .coalesce(10) 
      .write
      // Overwrite existing data if the job is run again
      .mode("overwrite")
      .parquet("hdfs:///s2orc_paper_data_cleaned/s2orc_paper_data_cleaned.parquet")

    // Stop the SparkSession to release resources
    spark.stop()
  }
}
