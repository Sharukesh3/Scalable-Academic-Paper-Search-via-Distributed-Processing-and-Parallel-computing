import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CoreDataCleanerSingleFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CORE Data Converter - Single File")
      .master("spark://asaicomputenode03.amritanet.edu:7077")
      .getOrCreate()

    import spark.implicits._

    // Read, select, and filter in a single chained command
    val cleanedDF = spark.read
      .json("hdfs:///core_2018_metadata_extracted/1.json")
      .select(
        $"title",
        $"abstract",
        $"authors",
        $"year",
        $"datePublished",
        $"doi",
        $"coreId",
        $"publisher",
        $"journals",
        $"subjects",
        $"topics",
        $"language".alias("language_name"),
        $"enrichments.documentType".getField("type").alias("docType"),
        $"enrichments.documentType".getField("confidence").alias("docTypeConfidence"),
        $"enrichments.references".alias("references"),
        $"identifiers"
      )
      .na.drop(Seq("title", "abstract"))

    // Now you can work with the final, cleaned DataFrame
    println(s"Number of cleaned rows in 1.json: ${cleanedDF.count()}")
    cleanedDF.show(5)
    
    // You could then proceed to write this small DataFrame if needed
    // cleanedDF.write.parquet("...")

    spark.stop()
  }
}
