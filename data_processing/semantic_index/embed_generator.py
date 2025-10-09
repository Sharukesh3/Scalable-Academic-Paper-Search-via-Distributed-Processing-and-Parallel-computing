# embed_generator.py (Updated for CORE 2018 fulltext schema)

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, concat_ws, PandasUDFType, lit, coalesce
from pyspark.sql.types import ArrayType, FloatType
from typing import Iterator, Iterable 

# --- 1. Configuration ---
MODEL_NAME = 'allenai/scibert_scivocab_uncased'
PARQUET_INPUT_PATH = "hdfs:///core_2018_fulltext_cleaned/cleaned_core_2018_fulltext.parquet"
PARQUET_OUTPUT_PATH = "hdfs:///core_2018_fulltext_semantic_embeddings_scibert/core_2018_fulltext__embeddings.parquet"


# --- 2. Spark Session ---
spark = SparkSession.builder \
    .appName("Core 2018 Embedding Generation") \
    .master("spark://asaicomputenode02.amritanet.edu:7077") \
    .getOrCreate()

# --- 3. Define the OPTIMIZED Embedding Generation UDF ---
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
            batch_size=2048 # Adjust based on GPU VRAM
        )
        yield pd.Series(list(embeddings))

# --- 4. Read, Process, and Write Data ---
print("Reading cleaned Parquet files...")
df = spark.read.parquet(PARQUET_INPUT_PATH)

# **FIXED**: Combine title and abstract, handling potential nulls in the abstract.
# No need to create a new ID column.
df_combined_text = df \
    .withColumn("abstract", coalesce(col("abstract"), lit(""))) \
    .withColumn("text_to_embed", concat_ws(" ", col("title"), col("abstract")))

print("Generating embeddings... This may take a while.")

# **FIXED**: Select the correct 'coreId' and apply the UDF.
df_embeddings = df_combined_text.withColumn(
    "embedding",
    generate_embeddings_udf(col("text_to_embed"))
).select("coreId", "title", "embedding") # Using 'coreId' from your schema

print(f"Writing embeddings and IDs to {PARQUET_OUTPUT_PATH}...")
df_embeddings.write.mode("overwrite").parquet(PARQUET_OUTPUT_PATH)

print("Job finished successfully!")
spark.stop()
