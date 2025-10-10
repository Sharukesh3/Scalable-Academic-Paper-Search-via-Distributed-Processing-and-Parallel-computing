import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, avg, lit, concat_ws
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, CountVectorizer
import pyspark.sql.functions as F

# --- 1. Configuration ---
INPUT_DATA_PATH = "hdfs:///s2orc_paper_data_cleaned/s2orc_paper_data_cleaned.parquet"
OUTPUT_STATS_PATH = "hdfs:///s2orc_bm_25/corpus_stats.json"
OUTPUT_FEATURES_PATH = "hdfs:///s2orc_bm_25/precomputed_features.parquet"
# Updated to match the vocabulary size of scibert_scivocab_uncased
VOCAB_SIZE = 50000

# --- 2. Spark Session ---
spark = SparkSession.builder \
    .appName("BM25 Pre-computation") \
    .master("spark://asaicomputenode02.amritanet.edu:7077") \
    .getOrCreate()

# --- 3. Load and Prepare Data ---
df = spark.read.parquet(INPUT_DATA_PATH)

# **FIXED**: Select 'corpusid' and only use 'title' for the text, as 'abstract' does not exist.
text_df = df \
    .select(col("corpusid"), col("title")) \
    .withColumn("text", col("title")) # Use title as the main text source

# --- 4. Feature Engineering Pipeline ---
# Tokenize, remove stop words
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

# --- 5. Calculate Average Document Length ---
print("Calculating average document length...")
doc_length_df = remover.transform(tokenizer.transform(text_df)) \
    .withColumn("doc_length", size(col("filtered_words")))

avg_doc_length = doc_length_df.select(avg("doc_length")).first()[0]
print(f"Average document length: {avg_doc_length}")

# --- 6. Calculate Term Frequencies (TF) and Inverse Document Frequencies (IDF) ---
print("Building vocabulary and calculating TF-IDF...")

# CountVectorizer builds a vocabulary from the most frequent words and gets term counts
cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=VOCAB_SIZE)
cv_model = cv.fit(doc_length_df)
featurized_df = cv_model.transform(doc_length_df)

# IDF model to get the IDF scores
idf = IDF(inputCol="raw_features", outputCol="features")
idf_model = idf.fit(featurized_df)

# The `idfModel.idf` attribute is a Spark Vector containing the IDF score for each word in the vocabulary
idf_scores = idf_model.idf.toArray()
vocabulary = cv_model.vocabulary

# Create a dictionary mapping term -> idf_score
idf_map = {term: float(score) for term, score in zip(vocabulary, idf_scores)}

# --- 7. Save Corpus Stats ---
print(f"Saving corpus stats to {OUTPUT_STATS_PATH}...")
corpus_stats = {
    "avgDocLength": avg_doc_length,
    "idf": idf_map,
    "vocabulary": vocabulary # Save the vocabulary to map query words to IDs
}

# Convert to a Spark DataFrame to save as a single JSON file
stats_json_str = json.dumps(corpus_stats)
spark.createDataFrame([(1, stats_json_str)], ["id", "json_stats"]) \
    .select("json_stats") \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .text(OUTPUT_STATS_PATH)

# --- 8. Save Precomputed Document Features for Searching ---
# We need to save the coreId, doc_length, and term frequencies for each document.
# The `raw_features` from CountVectorizer is a sparse vector of term counts.
# We will need to convert this to a map for the searcher.

def tf_vector_to_map(vector):
    # Converts a sparse vector of term counts into a {term_id: count} map
    return {int(i): int(v) for i, v in zip(vector.indices, vector.values)}

tf_vector_to_map_udf = F.udf(tf_vector_to_map, "map<int, int>")

final_features_df = featurized_df \
    .withColumn("term_freqs", tf_vector_to_map_udf(col("raw_features"))) \
    .select("corpusid", "doc_length", "term_freqs")

print(f"Saving precomputed features to {OUTPUT_FEATURES_PATH}...")
final_features_df.write.mode("overwrite").parquet(OUTPUT_FEATURES_PATH)

print("Pre-computation complete.")
spark.stop()

