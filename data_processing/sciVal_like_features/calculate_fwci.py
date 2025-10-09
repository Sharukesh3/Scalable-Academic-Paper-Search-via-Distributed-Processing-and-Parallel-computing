from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, udf
from pyspark.ml.clustering import KMeans
# Import the Vector types and UDF return type
from pyspark.ml.linalg import Vectors, VectorUDT

# --- 1. Configuration ---
ORIGINAL_DATA_PATH = "hdfs:///s2orc_paper_data_cleaned/s2orc_paper_data_cleaned.parquet"
EMBEDDINGS_PATH = "hdfs:///s2orc_semantic_embeddings/s2orc_embeddings.parquet"
# This is the crucial link you just created
LOOKUP_TABLE_PATH = "hdfs:///s2orc_id_lookup_table/lookup.parquet" 
FWCI_OUTPUT_PATH = "hdfs:///s2orc_fwci_scores/fwci_scores.parquet"

# K-Means parameter: number of research fields to create
NUM_CLUSTERS = 5000 

# --- 2. Spark Session ---
spark = SparkSession.builder \
    .appName("FWCI Calculation") \
    .master("spark://asaicomputenode02.amritanet.edu:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.17.16.11:9000") \
    .getOrCreate()

# --- Phase 1: Enrich Data using the Lookup Table ---
print("--- Loading and Enriching Data ---")
# Load original data with correct schema columns
metadata_df = spark.read.parquet(ORIGINAL_DATA_PATH) \
    .select(
        col("corpusid"),
        col("year").cast("int"),
        col("citationcount").cast("int")
    )

# Load embeddings and the lookup table
embeddings_df = spark.read.parquet(EMBEDDINGS_PATH).select("paper_id", "embedding")
lookup_df = spark.read.parquet(LOOKUP_TABLE_PATH)

print("--- Linking datasets using the lookup table ---")
# First, join embeddings with the lookup table to get corpusid
embeddings_with_corpusid = embeddings_df.join(lookup_df, "paper_id")

# Now, join with the metadata on the stable corpusid
enriched_df = metadata_df.join(embeddings_with_corpusid, "corpusid")

# --- Phase 2: Cluster Fields (K-Means) ---
print(f"\n--- Clustering into {NUM_CLUSTERS} Fields ---")

# Convert the 'embedding' array into a Spark ML Vector
to_vector_udf = udf(lambda r: Vectors.dense(r), VectorUDT())
enriched_df_for_clustering = enriched_df.withColumn("features", to_vector_udf(col("embedding")))

# NEW OPTIMIZATION: Take a random sample of the data for training K-Means.
# This will drastically reduce the shuffle size during the 'fit' operation.
# We'll use a 10% sample. This is a good balance between accuracy and performance.
print("Taking a 10% random sample for K-Means training...")
training_sample_df = enriched_df_for_clustering.sample(withReplacement=False, fraction=0.01, seed=42)
training_sample_df.cache() # Cache the sample for the iterative training process

# Update KMeans to use the new 'features' column
kmeans = KMeans().setK(NUM_CLUSTERS).setSeed(1).setFeaturesCol("features").setPredictionCol("field_id")
print("Fitting K-Means model on the sample (this is the most intensive step)...")
# Train ONLY on the smaller sample DataFrame
model = kmeans.fit(training_sample_df)
print("K-Means training complete.")

# Now, use the trained model to assign clusters to the FULL dataset
print("Assigning all papers to their respective fields...")
clustered_df_with_vectors = model.transform(enriched_df_for_clustering)

# Immediately drop the large vector columns. They are no longer needed.
clustered_df = clustered_df_with_vectors.select(
    "corpusid", 
    "year", 
    "citationcount", 
    "field_id"
)
# Cache the result as it will be used multiple times
clustered_df.cache()
print("Clustering complete. Vector columns have been dropped to save space.")


# --- Phase 3: Calculate Field Baselines ---
print("\n--- Calculating Field Baselines ---")
baseline_df = clustered_df \
    .groupBy("field_id", "year") \
    .agg(avg("citationcount").alias("avg_citations_for_field_year"))

# --- Phase 4: Compute FWCI ---
print("\n--- Computing FWCI for each paper ---")
final_df = clustered_df.join(baseline_df, ["field_id", "year"])
fwci_df = final_df.withColumn(
    "fwci",
    when(col("avg_citations_for_field_year") > 0, col("citationcount") / col("avg_citations_for_field_year"))
    .otherwise(0.0)
)

# --- 5. Save Results ---
print(f"\n--- Saving final data to {FWCI_OUTPUT_PATH} ---")
fwci_df.select("corpusid", "year", "citationcount", "field_id", "fwci") \
    .write.mode("overwrite").parquet(FWCI_OUTPUT_PATH)

print("Job finished successfully!")
spark.stop()
