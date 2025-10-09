from pyspark.sql import SparkSession
from pyspark.sql.functions import col, percent_rank
from pyspark.sql.window import Window

# --- 1. Configuration ---
# This is the output from your FWCI job.
FWCI_INPUT_PATH = "hdfs:///s2orc_fwci_scores/fwci_scores.parquet"

# This is the final output path for data enriched with percentiles.
PERCENTILE_OUTPUT_PATH = "hdfs:///s2orc_percentile_scores/percentile_scores.parquet"

# --- 2. Spark Session ---
spark = SparkSession.builder \
    .appName("Citation Percentile Calculation") \
    .master("spark://asaicomputenode02.amritanet.edu:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.17.16.11:9000") \
    .getOrCreate()

# --- 3. Load FWCI Data ---
print(f"--- Loading data from {FWCI_INPUT_PATH} ---")
fwci_df = spark.read.parquet(FWCI_INPUT_PATH)

# --- 4. Calculate Percentiles using a Window Function ---
print("--- Calculating citation percentiles for each field and year ---")

# Define the "window" or group to calculate ranks over.
# We partition by field and year, and order by citation count descending.
windowSpec = Window.partitionBy("field_id", "year").orderBy(col("citationcount").desc())

# The percent_rank() function gives the percentile of each paper within its group.
# A value of 0.0 is the top-ranked (most cited), a value close to 1.0 is the lowest ranked.
percentile_df = fwci_df.withColumn("percentile_rank", percent_rank().over(windowSpec))

print("Percentile calculation complete.")

# --- 5. Save the Final Results ---
print(f"--- Saving final data with percentiles to {PERCENTILE_OUTPUT_PATH} ---")
percentile_df.write.mode("overwrite").parquet(PERCENTILE_OUTPUT_PATH)

print("Job finished successfully!")
spark.stop()

