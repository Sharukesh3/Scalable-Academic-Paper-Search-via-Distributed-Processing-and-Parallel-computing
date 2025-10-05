import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, lit
from pyspark.sql.types import FloatType
from bm25_scorer import BM25Scorer
from typing import Iterator, Iterable

# --- 1. Configuration ---
# NOTE: These paths would point to the output of your pre-computation job
PRECOMPUTED_FEATURES_PATH = "hdfs:///s2orc_lexical_index/lexical_index_title.parquet"
# This would be a small file containing corpus-wide stats
CORPUS_STATS_PATH = "hdfs:///path/to/your/corpus_stats.json" 

QUERY_TEXT = "machine learning in medicine"
K1 = 1.5
B = 0.75

# --- 2. Spark Session ---
spark = SparkSession.builder \
    .appName("BM25 Searcher with CUDA") \
    .master("spark://asaicomputenode03.amritanet.edu:7077") \
    .getOrCreate()

# --- 3. Mock Data Generation (Replace with actual data loading) ---
# In a real scenario, you would load pre-computed data.
# For this example, we'll create mock data to demonstrate the process.
print("Creating mock data (replace this with HDFS loading)...")
# Let's assume we have a vocabulary: {'machine':0, 'learning':1, 'medicine':2, 'paper':3}
# Doc 1: "machine learning paper"
# Doc 2: "learning in medicine"
# Doc 3: "a paper about medicine"
mock_data = [
    (101, 15, {0: 1, 1: 1, 3: 1}), # coreId, doc_length, {term_id: tf}
    (102, 18, {1: 1, 2: 1}),
    (103, 22, {3: 1, 2: 1}),
]
precomputed_df = spark.createDataFrame(mock_data, ["coreId", "doc_length", "term_freqs"])

# Mock corpus stats
# In reality, you'd calculate these once with Spark
avg_doc_length = 18.3
# IDF for query terms: 'machine', 'learning', 'medicine'
idf_map = {0: 0.8, 1: 0.5, 2: 0.9} 
# Query terms converted to their IDs
query_term_ids = np.array([0, 1, 2], dtype=np.int32)
idf_scores = np.array([idf_map[tid] for tid in query_term_ids], dtype=np.float32)

# --- 4. Define the CUDA-powered Pandas UDF ---
# Broadcast the large, shared data to each executor once
broadcast_query_ids = spark.sparkContext.broadcast(query_term_ids)
broadcast_idf_scores = spark.sparkContext.broadcast(idf_scores)

@pandas_udf(FloatType())
def calculate_bm25_udf(iterator: Iterator[pd.DataFrame]) -> Iterable[pd.Series]:
    # This runs once per task: initialize the CUDA scorer
    scorer = BM25Scorer()
    
    # Get the broadcasted query data
    query_ids = broadcast_query_ids.value
    idfs = broadcast_idf_scores.value
    
    for pdf in iterator:
        # --- Prepare data for CUDA kernel ---
        doc_lengths = pdf['doc_length'].to_numpy(dtype=np.int32)
        
        # Flatten the term frequencies into a format the kernel can read
        docs_terms_list = []
        doc_offsets_list = [0]
        offset = 0
        for term_map in pdf['term_freqs']:
            for term_id, tf in term_map.items():
                docs_terms_list.append((term_id, float(tf)))
            offset += len(term_map)
            doc_offsets_list.append(offset)
            
        docs_terms = np.array(docs_terms_list, dtype=[('id', 'i4'), ('tf', 'f4')])
        doc_offsets = np.array(doc_offsets_list, dtype=np.int32)

        # --- Call the CUDA Scorer ---
        scores = scorer.score_batch(
            docs_terms, doc_offsets, doc_lengths, 
            query_ids, idfs, avg_doc_length, K1, B
        )
        
        yield pd.Series(scores)

# --- 5. Run the Search ---
print("Running BM25 search with CUDA acceleration...")
results_df = precomputed_df.withColumn(
    "bm25_score",
    calculate_bm25_udf(col("doc_length"), col("term_freqs"))
)

print("Search complete. Top results:")
results_df.orderBy(col("bm25_score").desc()).show()

spark.stop()
