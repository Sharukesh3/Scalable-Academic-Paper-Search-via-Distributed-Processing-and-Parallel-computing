# search.py
import faiss
import numpy as np
import pickle
from sentence_transformers import SentenceTransformer

# --- 1. Load Everything ---
print("Loading model, index, and mapping...")
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
index = faiss.read_index("s2orc_index.faiss")
with open("s2orc_id_mapping.pkl", 'rb') as f:
    id_title_mapping = pickle.load(f)

# --- 2. Perform a Search ---
query_text = "The impact of machine learning on medical diagnostics"
k = 5 # Number of results to retrieve

print(f"\nSearching for top {k} results for query: '{query_text}'")

# Convert query to vector
query_vector = model.encode([query_text], normalize_embeddings=True).astype('float32')

# Search the index
# D = distances, I = indices
distances, indices = index.search(query_vector, k)

# --- 3. Display Results ---
print("\nResults:")
for i, idx in enumerate(indices[0]):
    # Use the mapping to get the original paper_id and title
    original_paper_id, title = id_title_mapping[idx]
    distance = distances[0][i]
    print(f"{i+1}. Title: {title}")
    print(f"   (FAISS Index: {idx}, Paper ID: {original_paper_id}, Distance: {distance:.4f})")
