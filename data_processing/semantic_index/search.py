import faiss
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
import pickle
import os

# --- 1. Configuration ---
MODEL_NAME = 'allenai/scibert_scivocab_uncased' 
INDEX_PATH = "s2orc_scibert_index_ivfpq.faiss"
# This script is designed to use the richer mapping file with both IDs and titles.
MAPPING_PATH = "s2orc_scibert_mapping.pkl"

# --- 2. Search Parameters ---
QUERY_TEXT = "The role of neural networks in analyzing genomic data"
TOP_K = 10 

# --- 3. Load the Index and Mapping ---
print("Loading model, index, and mapping...")
if not os.path.exists(INDEX_PATH) or not os.path.exists(MAPPING_PATH):
    print(f"Error: Index ('{INDEX_PATH}') or Mapping ('{MAPPING_PATH}') file not found.")
    print("Please ensure you have run the 'build_index_production.py' script that creates the .pkl file.")
    exit()

model = SentenceTransformer(MODEL_NAME)
index = faiss.read_index(INDEX_PATH)

# Load the dictionary mapping from the pickle file
with open(MAPPING_PATH, 'rb') as f:
    id_and_title_map = pickle.load(f)

# --- 4. Set the nprobe Parameter (Crucial for IVF Index) ---
# nprobe is the number of partitions to search. Higher is more accurate but slower.
index.nprobe = 64
print(f"Set index.nprobe to {index.nprobe} for searching.")

# --- 5. Perform the Search ---
print(f"\nSearching for top {TOP_K} results for query: '{QUERY_TEXT}'")
query_vector = model.encode([QUERY_TEXT], normalize_embeddings=True).astype('float32')
distances, indices = index.search(query_vector, TOP_K)

# --- 6. Display Results ---
# The title lookup is now fast and simple, reading directly from the loaded dictionary.
print("\n--- SciBERT Search Results ---")
for i, idx in enumerate(indices[0]):
    # Look up the paper_id and title directly from the mapping dictionary
    # The dictionary maps: FAISS_index_position -> (paper_id, title)
    paper_id, title = id_and_title_map.get(idx, (-1, "Title not found for this index position"))
    distance = distances[0][i] 
    
    print(f"{i+1}. Title: {title}")
    print(f"   (Paper ID: {paper_id}, Approx. Distance: {distance:.4f})")

