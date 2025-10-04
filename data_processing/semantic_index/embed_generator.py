# embed_generator.py (final version)

import pandas as pd
from pyspark.sql import SparkSession
# CHANGE 1: Import PandasUDFType
from pyspark.sql.functions import pandas_udf, col, concat_ws, monotonically_increasing_id, PandasUDFType
from pyspark.sql.types import ArrayType, FloatType
from typing import Iterator, Iterable 

# --- 1. Configuration ---
MODEL_NAME = 'allenai/scibert_scivocab_uncased'
# Make sure to use your full HDFS path here if needed
PARQUET_INPUT_PATH = "hdfs:///s2orc_paper_data_cleaned/s2orc_paper_data_cleaned.parquet"
PARQUET_OUTPUT_PATH = "hdfs:///s2orc_semantic_embeddings_scibert/s2orc_embeddings.parquet"


# --- 2. Spark Session ---
spark = SparkSession.builder \
    .appName("S2ORC Embedding Generation") \
    .master("spark://asaicomputenode03.amritanet.edu:7077") \
    .getOrCreate()

# --- 3. Define the OPTIMIZED Embedding Generation UDF ---
# CHANGE 2: Add the PandasUDFType.SCALAR_ITER type
@pandas_udf(ArrayType(FloatType()), PandasUDFType.SCALAR_ITER)
def generate_embeddings_udf(series_iter: Iterator[pd.Series]) -> Iterable[pd.Series]:
    # Import and load the model ONCE per task
    from sentence_transformers import SentenceTransformer
    
    # Get the context and set the device (same as before)
    from pyspark import TaskContext
    ctx = TaskContext.get()
    if ctx and ctx.resources() and 'gpu' in ctx.resources():
        gpu_id = ctx.resources()['gpu'].addresses[0]
        device = f"cuda:{gpu_id}"
    else:
        device = "cpu"
        
    model = SentenceTransformer(MODEL_NAME, device=device)
    
    # Process each pandas Series in the iterator
    for series in series_iter:
        # The encode method processes the entire batch efficiently
        embeddings = model.encode(
            series.tolist(), 
            normalize_embeddings=True,
            batch_size=4096 # Adjust based on GPU VRAM
        )
        yield pd.Series(list(embeddings))

# --- 4. Read, Process, and Write Data ---
print("Reading cleaned Parquet files...")
df = spark.read.parquet(PARQUET_INPUT_PATH)

# Add a unique, stable ID to each row for later mapping.
df_with_id = df.withColumn("paper_id", monotonically_increasing_id())

# Use only the title for embedding
df_combined_text = df_with_id.withColumn(
    "text_to_embed",
    col("title")
)

print("Generating embeddings... This may take a while.")
# Select the ID and apply the UDF to generate embeddings.
df_embeddings = df_combined_text.withColumn(
    "embedding",
    generate_embeddings_udf(col("text_to_embed"))
).select("paper_id", "title", "embedding")

print(f"Writing embeddings and IDs to {PARQUET_OUTPUT_PATH}...")
df_embeddings.write.mode("overwrite").parquet(PARQUET_OUTPUT_PATH)

print("Job finished successfully!")
spark.stop()
