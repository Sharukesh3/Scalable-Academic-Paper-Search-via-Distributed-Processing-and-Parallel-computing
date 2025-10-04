import faiss
import numpy as np
import pandas as pd
import glob
import os

# --- 1. Configuration ---
EMBEDDINGS_DIR = 's2orc_scibert_embeddings_local/'
INDEX_SAVE_PATH = "s2orc_scibert_index_ivfpq.faiss"
MAPPING_SAVE_PATH = "s2orc_scibert_paper_ids.npy"

# --- 2. Get the list of Parquet files ---
parquet_files = sorted(glob.glob(os.path.join(EMBEDDINGS_DIR, '*.parquet')))
print(f"Found {len(parquet_files)} Parquet files.")
if not parquet_files:
    print("No Parquet files found. Exiting.")
    exit()

# --- 3. Prepare for Training the Index ---
# For IVFPQ, we can use a smaller training set. Let's use just 2 files.
print("Reading a smaller sample of data for training...")
training_sample_list = []
num_files_for_training = min(2, len(parquet_files)) # <-- Reduced training sample size
for file_path in parquet_files[:num_files_for_training]:
    df_sample = pd.read_parquet(file_path)
    training_sample_list.extend(df_sample['embedding'].tolist())

training_vectors = np.array(training_sample_list).astype('float32')
d = training_vectors.shape[1]
print(f"Vector dimension is {d}. Training on {len(training_vectors)} vectors.")

# --- 4. Create and Train the IndexIVFPQ Index ---
nlist = 4096  # More partitions are better for larger datasets
m = 64      # The number of sub-vectors each vector is split into. Must be a divisor of d.
nbits = 8   # The number of bits per sub-vector code. 8 is standard.

# The quantizer is the same as before
quantizer = faiss.IndexFlatL2(d)
# Create the IndexIVFPQ index
index = faiss.IndexIVFPQ(quantizer, d, nlist, m, nbits)

print(f"Training the IVFPQ index with {nlist} partitions...")
index.train(training_vectors)
print("Training complete. Index is ready to be populated.")

# --- 5. Populate the Index and Create ID Mapping ---
ordered_paper_ids = []
for i, file_path in enumerate(parquet_files):
    print(f"Processing and adding file {i+1}/{len(parquet_files)}...")
    df = pd.read_parquet(file_path)
    
    embeddings = np.array(df['embedding'].tolist()).astype('float32')
    index.add(embeddings)
    
    ordered_paper_ids.extend(df['paper_id'].tolist())

paper_id_map_array = np.array(ordered_paper_ids)

print(f"\nIndex is built. Total vectors in index: {index.ntotal}")

# --- 6. Save the Index and the Paper ID Mapping ---
print(f"Saving FAISS index to {INDEX_SAVE_PATH}")
faiss.write_index(index, INDEX_SAVE_PATH)

print(f"Saving paper ID mapping to {MAPPING_SAVE_PATH}")
np.save(MAPPING_SAVE_PATH, paper_id_map_array)

print("Done.")
