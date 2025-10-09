from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import pyarrow.parquet as pq
import pandas as pd
import os
import glob
from contextlib import asynccontextmanager

# --- 1. Configuration ---
MODEL_NAME = 'allenai/scibert_scivocab_uncased' 
INDEX_PATH = "/dist_home/suryansh/BD/data_processing/semantic_index/s2orc_scibert_index_ivfpq.faiss"
MAPPING_PATH = "/dist_home/suryansh/BD/data_processing/semantic_index/s2orc_scibert_paper_ids.npy"
ORIGINAL_DATA_PATH = "/dist_home/suryansh/BD/data_processing/semantic_index/s2orc_cleaned_data_local/"
LOOKUP_TABLE_PATH = "/dist_home/suryansh/BD/data_processing/semantic_index/s2orc_id_lookup_table_local/"
GPU_ID = 0 

# --- 2. Data Models for the API ---
class SearchQuery(BaseModel):
    query: str
    top_k: int = 10

class SearchResult(BaseModel):
    paper_id: int
    corpus_id: int | None
    title: str
    distance: float

# --- 3. Global dictionary to hold loaded models ---
api_globals = {}

# --- 4. Lifespan Function (Loads models and data at startup) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Loading models and index...")
    api_globals['model'] = SentenceTransformer(MODEL_NAME)
    
    print(f"Memory-mapping CPU index from {INDEX_PATH}...")
    cpu_index = faiss.read_index(INDEX_PATH, faiss.IO_FLAG_MMAP)
    
    print(f"Moving index to GPU: {GPU_ID} with optimized settings...")
    res = faiss.StandardGpuResources()
    options = faiss.GpuClonerOptions()
    options.useFloat16 = True # Solves the shared memory error
    
    gpu_index = faiss.index_cpu_to_gpu(res, GPU_ID, cpu_index, options)
    api_globals['index'] = gpu_index
    api_globals['index'].nprobe = 64
    
    print(f"Loading paper ID mapping from {MAPPING_PATH}...")
    api_globals['paper_id_map'] = np.load(MAPPING_PATH)
    
    print("Startup complete. API is ready to accept requests.")
    yield
    api_globals.clear()
    print("Shutdown complete.")

app = FastAPI(lifespan=lifespan)

# --- 5. The Search API Endpoint ---
@app.post("/search", response_model=list[SearchResult])
async def search(query: SearchQuery):
    # Retrieve all necessary components from the global context
    model = api_globals['model']
    index = api_globals['index']
    paper_id_map = api_globals['paper_id_map']

    # 1. Encode query
    query_vector = model.encode([query.query], normalize_embeddings=True).astype('float32')
    
    # 2. Search FAISS index on the GPU
    distances, indices = index.search(query_vector, query.top_k)
    
    result_indices = indices[0]
    # Get the artificial paper_ids from the mapping array
    top_paper_ids = [int(paper_id_map[i]) for i in result_indices]

    # 3. Perform an efficient, on-demand, two-step lookup for titles by scanning files
    
    # Step A: Find the corresponding corpusids by scanning the lookup table files
    paper_to_corpus_id_map = {}
    all_lookup_files = glob.glob(os.path.join(LOOKUP_TABLE_PATH, '*.parquet'))
    for file_path in all_lookup_files:
        df = pd.read_parquet(file_path, columns=['paper_id', 'corpusid'])
        filtered_df = df[df['paper_id'].isin(top_paper_ids)]
        if not filtered_df.empty:
            paper_to_corpus_id_map.update(filtered_df.set_index('paper_id')['corpusid'].to_dict())
    top_corpus_ids = list(paper_to_corpus_id_map.values())

    # Step B: Use the corpusids to get the titles by scanning the original data files
    corpus_to_title_map = {}
    all_original_files = glob.glob(os.path.join(ORIGINAL_DATA_PATH, '*.parquet'))
    for file_path in all_original_files:
        df = pd.read_parquet(file_path, columns=['corpusid', 'title'])
        filtered_df = df[df['corpusid'].isin(top_corpus_ids)]
        if not filtered_df.empty:
            corpus_to_title_map.update(filtered_df.set_index('corpusid')['title'].to_dict())
    
    # 4. Format and return results
    results = []
    for i, paper_id in enumerate(top_paper_ids):
        corpus_id = paper_to_corpus_id_map.get(paper_id)
        title = corpus_to_title_map.get(corpus_id, "Title not found") if corpus_id else "Title not found"
        
        results.append(SearchResult(
            paper_id=paper_id,
            corpus_id=corpus_id,
            title=title,
            distance=float(distances[0][i])
        ))
    return results

@app.get("/")
def read_root():
    return {"message": "Scientific Paper Search API is running. Send a POST request to /search."}

