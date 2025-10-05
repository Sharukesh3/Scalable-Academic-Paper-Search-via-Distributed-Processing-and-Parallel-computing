from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- 1. Configuration ---
# Path to your EXISTING embeddings with the "bad" paper_id
EXISTING_EMBEDDINGS_PATH = "hdfs:///s2orc_semantic_embeddings/s2orc_embeddings.parquet"

# Path to the lookup table you just created
LOOKUP_TABLE_PATH = "hdfs:///s2orc_id_lookup_table/lookup.parquet"

# The output path for your NEW, corrected embeddings file
ENRICHED_EMBEDDINGS_PATH = "hdfs:///s2orc_embeddings_enriched/enriched_embeddings.parquet"

# --- 2. Spark Session ---
spark = SparkSession.builder \
    .appName("Enrich Embeddings with Corpus ID") \
    .master("spark://asaicomputenode02.amritanet.edu:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.17.16.11:9000") \
    .getOrCreate()

# --- 3. Load the data ---
print(f"Loading existing embeddings from {EXISTING_EMBEDDINGS_PATH}...")
embeddings_df = spark.read.parquet(EXISTING_EMBEDDINGS_PATH)

print(f"Loading lookup table from {LOOKUP_TABLE_PATH}...")
lookup_df = spark.read.parquet(LOOKUP_TABLE_PATH)

# --- 4. Join to add the corpusid and fix the schema ---
# We are reverting to a standard join. Spark will use a shuffle-based join
# strategy that can handle large tables without overwhelming the driver.
print("Joining embeddings with the lookup table using a shuffle join...")
enriched_df = embeddings_df.join(lookup_df, "paper_id") \
    .select(
        col("corpusid"), # The "good" ID is now the primary identifier
        col("title"),
        col("embedding")
    )

# --- 5. Save the new, enriched dataset ---
print(f"Saving new, enriched embeddings to {ENRICHED_EMBEDDINGS_PATH}...")
enriched_df.write.mode("overwrite").parquet(ENRICHED_EMBEDDINGS_PATH)

print(f"Successfully created the enriched embeddings file with {enriched_df.count()} entries.")
print("You can now use this new file for all downstream tasks.")

spark.stop()

