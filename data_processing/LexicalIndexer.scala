import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * A Spark application to build a lexical index from the cleaned CORE 2018 dataset
 * using 'coreId', 'title', and 'abstract'.
 */
object LexicalIndexer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lexical Indexer for CORE 2018")
      .master("spark://asaicomputenode03.amritanet.edu:7077")
      .getOrCreate()

    import spark.implicits._

    // --- 1. Load the Cleaned Parquet Data from the new path ---
    val cleanedDF = spark.read.parquet("hdfs:///core_2018_metadata_cleaned/cleaned_core_2018_metadata.parquet")

    // --- 2. Prepare Data for Indexing ---
    // Select the specified columns and combine title and abstract.
    val textDF = cleanedDF
      .select($"coreId", $"title", $"abstract")
      .withColumn("abstract", coalesce($"abstract", lit(""))) // Replace null abstracts with empty string
      .withColumn("text", concat_ws(" ", $"title", $"abstract")) // Combine title and abstract
      .select("coreId", "text")
      .filter($"text".isNotNull.and(trim($"text") =!= "")) // Ensure text is not null or empty

    // --- 3. Define the ML Pipeline Stages ---

    // Stage 1: Tokenize the 'text' column into a 'words' column.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    // Stage 2: Remove stop words from the 'words' column.
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered_words")

    // Stage 3: Apply HashingTF to convert words to term frequency vectors.
    val hashingTF = new HashingTF()
      .setInputCol("filtered_words")
      .setOutputCol("raw_features")
      .setNumFeatures(20000)

    // Stage 4: Apply IDF to weigh the terms.
    val idf = new IDF()
      .setInputCol("raw_features")
      .setOutputCol("features") // This is the final TF-IDF vector

    // --- 4. Assemble and Run the Pipeline ---
    val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf))

    println("Fitting the TF-IDF pipeline model for CORE 2018...")
    val pipelineModel = pipeline.fit(textDF)
    println("Model fitting complete.")

    println("Transforming data to create the index...")
    val indexedDF = pipelineModel.transform(textDF)
    println("Data transformation complete.")

    // --- 5. Save the Model and the Indexed Data to new HDFS paths ---
    
    // Save the trained pipeline model.
    pipelineModel.write.overwrite().save("hdfs:///core_2018_metadata_models/tfidf_pipeline_model")

    // Save the indexed data (papers with their TF-IDF vectors).
    indexedDF
      .select("coreId", "features")
      .coalesce(20) // Consolidate into a reasonable number of files
      .write
      .mode("overwrite")
      .parquet("hdfs:///core_2018_metadata_lexical_index/lexical_index.parquet")
    
    println("Successfully saved the CORE 2018 pipeline model and lexical index.")

    spark.stop()
  }
}
