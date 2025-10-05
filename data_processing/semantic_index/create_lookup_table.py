from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- 1. Configuration ---
# Path to your EXISTING embeddings with the artificial paper_id
EMBEDDINGS_PATH = "hdfs:///s2orc_semantic_embeddings/s2orc_embeddings.parquet"

# Path to your original cleaned data with the stable corpusid
ORIGINAL_DATA_PATH = "hdfs:///s2orc_paper_data_cleaned/s2orc_paper_data_cleaned.parquet"

# The output path for our new, crucial lookup table
LOOKUP_TABLE_PATH = "hdfs:///s2orc_id_lookup_table/lookup.parquet"

# --- 2. Spark Session ---
spark = SparkSession.builder \
    .appName("Create Paper ID to Corpus ID Lookup Table") \
    .master("spark://asaicomputenode02.amritanet.edu:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.17.16.11:9000") \
    .getOrCreate()

# --- 3. Load the necessary columns from both datasets ---
print(f"Loading data from {EMBEDDINGS_PATH}...")
embeddings_ids_df = spark.read.parquet(EMBEDDINGS_PATH).select("paper_id", "title")

print(f"Loading data from {ORIGINAL_DATA_PATH}...")
original_ids_df = spark.read.parquet(ORIGINAL_DATA_PATH).select("corpusid", "title")

# --- 4. Join the two DataFrames on the 'title' column ---
# This links the artificial paper_id to the stable corpusid.
# We drop duplicates in case of multiple papers having the exact same title.
print("Joining datasets on 'title' to create the lookup table...")
lookup_df = embeddings_ids_df.join(original_ids_df, "title") \
    .select("paper_id", "corpusid") \
    .dropDuplicates(["paper_id"])

# --- 5. Save the lookup table ---
print(f"Saving the lookup table to {LOOKUP_TABLE_PATH}...")
lookup_df.write.mode("overwrite").parquet(LOOKUP_TABLE_PATH)

print(f"Successfully created the ID lookup table with {lookup_df.count()} entries.")
print("You can now proceed with the FWCI calculation without re-running the embedding job.")

spark.stop()
