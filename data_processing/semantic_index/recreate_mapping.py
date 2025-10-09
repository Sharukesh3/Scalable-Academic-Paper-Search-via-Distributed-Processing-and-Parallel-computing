import numpy as np
import pandas as pd
import glob
import os

# --- 1. Configuration ---
# This should point to the directory containing your local Parquet embedding files
EMBEDDINGS_DIR = 's2orc_scibert_embeddings_local/' 
# The output path for the .npy file that your API needs
OUTPUT_MAPPING_PATH = "/dist_home/suryansh/BD/data_processing/semantic_index/s2orc_scibert_paper_ids.npy"

# --- 2. Get the list of Parquet files ---
# Ensure the files are processed in the same order as when the index was built
parquet_files = sorted(glob.glob(os.path.join(EMBEDDINGS_DIR, '*.parquet')))
if not parquet_files:
    print(f"Error: No Parquet files found in '{EMBEDDINGS_DIR}'. Please check the path.")
    exit()

print(f"Found {len(parquet_files)} Parquet files. Recreating the ID mapping...")

# --- 3. Recreate the Mapping ---
# This will hold the paper IDs in the exact same order as they were added to the FAISS index.
ordered_paper_ids = []

for i, file_path in enumerate(parquet_files):
    print(f"Processing file {i+1}/{len(parquet_files)}...")
    # Read ONLY the 'paper_id' column to save memory and time
    df = pd.read_parquet(file_path, columns=['paper_id'])
    
    # Append the paper IDs from this file to our list
    ordered_paper_ids.extend(df['paper_id'].tolist())

# Convert the Python list to a more efficient NumPy array
paper_id_map_array = np.array(ordered_paper_ids)

# --- 4. Save the NumPy array ---
print(f"\nSaving paper ID mapping to {OUTPUT_MAPPING_PATH}...")
np.save(OUTPUT_MAPPING_PATH, paper_id_map_array)

print("Done. The s2orc_scibert_paper_ids.npy file has been recreated successfully.")
print("You can now restart your API server.")
