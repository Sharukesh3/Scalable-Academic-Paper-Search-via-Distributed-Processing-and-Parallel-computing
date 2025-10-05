import faiss
import numpy as np
import pandas as pd
import glob
import os
import pickle

# --- 1. Configuration ---
EMBEDDINGS_DIR = 's2orc_scibert_embeddings_local/'
INDEX_SAVE_PATH = "s2orc_scibert_index_ivfpq.faiss"
# We will create a richer mapping file containing both IDs and titles
MAPPING_SAVE_PATH = "s2orc_scibert_mapping.pkl" 

# --- 2. Get the list of Parquet files ---
parquet_files = sorted(glob.glob(os.path.join(EMBEDDINGS_DIR, '*.parquet')))
print(f"Found {len(parquet_files)} Parquet files.")
if not parquet_files:
    print("No Parquet files found. Exiting.")
    exit()

# --- 3. Prepare for Training the Index ---
# We use a small, manageable sample of the data to train the index partitions.
print("Reading a small sample of data for training...")
training_sample_list = []
# Use 5 files for a robust training set. This is a good balance.
num_files_for_training = min(5, len(parquet_files)) 
for file_path in parquet_files[:num_files_for_training]:
    df_sample = pd.read_parquet(file_path)
    training_sample_list.extend(df_sample['embedding'].tolist())

training_vectors = np.array(training_sample_list).astype('float32')
d = training_vectors.shape[1] # Should be 768 for SciBERT
print(f"Vector dimension is {d}. Training on {len(training_vectors)} vectors.")

# --- 4. Create and Train the IndexIVFPQ Index ---
# These are the parameters for the production-grade index.
nlist = 4096  # Number of partitions (cells)
m = 64      # Number of sub-quantizers for vector compression
nbits = 8   # Bits per sub-quantizer code (8 is standard)

# The quantizer is a simple index used to find the right partition.
quantizer = faiss.IndexFlatL2(d)
# Create the IndexIVFPQ index, which is memory-efficient and fast.
index = faiss.IndexIVFPQ(quantizer, d, nlist, m, nbits)

print(f"Training the IVFPQ index with {nlist} partitions...")
index.train(training_vectors)
print("Training complete. Index is ready to be populated.")

# --- 5. Populate the Index and Create the Rich Mapping ---
# We will create a dictionary that maps: index_position -> (paper_id, title)
id_and_title_mapping = {}
current_index_pos = 0

# Process each file one by one to keep memory usage low
for i, file_path in enumerate(parquet_files):
    print(f"Processing and adding file {i+1}/{len(parquet_files)}: {os.path.basename(file_path)}...")
    df = pd.read_parquet(file_path)
    
    # Add the vectors from this file to the index
    embeddings = np.array(df['embedding'].tolist()).astype('float32')
    index.add(embeddings)
    
    # Update the mapping with both paper_id and title from this file
    for row in df.itertuples():
        id_and_title_mapping[current_index_pos] = (row.paper_id, row.title)
        current_index_pos += 1

print(f"\nIndex is built. Total vectors in index: {index.ntotal}")

# --- 6. Save the Index and the New, Rich Mapping ---
print(f"Saving FAISS index to {INDEX_SAVE_PATH}")
faiss.write_index(index, INDEX_SAVE_PATH)

print(f"Saving ID and Title mapping to {MAPPING_SAVE_PATH}")
with open(MAPPING_SAVE_PATH, 'wb') as f:
    pickle.dump(id_and_title_mapping, f)

print("Done.")
