import json
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, explode, lit, spark_partition_id
# Import the necessary UDF type
from pyspark.sql.functions import PandasUDFType
from pyspark.sql.types import StructType, StructField, LongType, FloatType
from scipy.sparse import csr_matrix
from bm25_scorer import BM25Scorer

# --- 1. Configuration ---
# **FIXED**: Updated paths to match the output of your pre-computation job
CORPUS_STATS_PATH = "hdfs:///s2orc_bm_25/corpus_stats.json"
FEATURES_PATH = "hdfs:///s2orc_bm_25/precomputed_features.parquet"
# This path is for looking up the final titles
ORIGINAL_DATA_PATH = "hdfs:///s2orc_paper_data_cleaned/s2orc_paper_data_cleaned.parquet"


# --- 2. Search Parameters ---
QUERY_TEXT = "machine learning in medicine"
TOP_N = 10
BM25_K1 = 1.5
BM25_B = 0.75

# --- 3. Spark Session ---
spark = SparkSession.builder \
    .appName("BM25 Searcher") \
    .master("spark://asaicomputenode02.amritanet.edu:7077") \
    .getOrCreate()

# --- 4. Load Pre-computed Data ---
print("Loading pre-computed corpus data...")
# Load the JSON stats into a Python dictionary
stats_rdd = spark.read.text(CORPUS_STATS_PATH).rdd
stats_json_str = stats_rdd.map(lambda row: row.value).collect()[0]
corpus_stats = json.loads(stats_json_str)

avg_doc_length = corpus_stats["avgDocLength"]
idf_map = corpus_stats["idf"]
vocabulary = corpus_stats["vocabulary"]
# Create a reverse map for term -> id
vocab_map = {term: i for i, term in enumerate(vocabulary)}
# Create an ordered array of IDF scores based on term ID
idf_scores_array = np.array([idf_map.get(term, 0.0) for term in vocabulary], dtype=np.float32)

# Load the document features (TF, doc length)
features_df = spark.read.parquet(FEATURES_PATH)

# --- 5. Prepare Query ---
print(f"Processing query: '{QUERY_TEXT}'")
query_terms = [word for word in QUERY_TEXT.lower().split() if word in vocab_map]
query_term_ids = np.array([vocab_map[word] for word in query_terms], dtype=np.int32)

# --- 6. Define the Scoring UDF (with correct type) ---
# Define the schema for the output of our UDF
result_schema = StructType([
    StructField("coreId", LongType(), True),
    StructField("score", FloatType(), True)
])

# **FIXED**: Removed the redundant @pandas_udf decorator.
# The applyInPandas function will handle the UDF registration.
def score_partition(df: pd.DataFrame) -> pd.DataFrame:
    """
    This UDF will run on each partition of the data on an executor.
    """
    # Initialize the CUDA scorer ONCE per partition
    scorer = BM25Scorer()
    
    # Extract data from the pandas DataFrame
    doc_ids = df['coreId'].to_numpy()
    doc_lengths = df['doc_length'].to_numpy(dtype=np.int32)
    
    # Reconstruct the sparse matrix for term frequencies from the map
    rows, cols, data = [], [], []
    for i, tf_map in enumerate(df['term_freqs']):
        if tf_map is None: continue # Skip if a document has no terms
        for term_id, freq in tf_map.items():
            rows.append(i)
            cols.append(term_id)
            data.append(freq)
            
    tfs_matrix = csr_matrix((data, (rows, cols)), shape=(len(df), len(vocabulary)))

    # Call the CUDA scorer
    scores = scorer.score(
        query_term_ids,
        doc_lengths,
        tfs_matrix,
        idf_scores_array,
        k1=BM25_K1,
        b=BM25_B,
        avg_doc_length=avg_doc_length
    )
    
    # Return the results as a new pandas DataFrame
    return pd.DataFrame({'coreId': doc_ids, 'score': scores})

# --- 7. Run the Search ---
print("Scoring documents using CUDA kernel...")
# We use `applyInPandas` because we have a Grouped Map UDF
# Add a partition ID column to group on. We repartition to 1 to ensure all data goes to one GPU.
results_df = features_df.repartition(1) \
    .withColumn("partition_id", spark_partition_id()) \
    .groupby("partition_id") \
    .applyInPandas(score_partition, schema=result_schema)

# --- 8. Get Top Results and Join for Titles ---
print("Finding top results...")
top_results_df = results_df.orderBy(col("score").desc()).limit(TOP_N)

# Load original data to get titles
original_df = spark.read.parquet(ORIGINAL_DATA_PATH).select("corpusid", "title")

# Join with original data to show titles (aliasing coreId to corpusid for the join)
final_results = top_results_df.join(
    original_df,
    top_results_df.coreId == original_df.corpusid
).select("coreId", "title", "score")

print(f"\n--- Top {TOP_N} BM25 Results ---")
final_results.show(truncate=False)

spark.stop()

