# --- 1. Imports ---
# General
import os
import pickle
import glob
import json
import uuid
import subprocess
from typing import List, Optional

# Async and Web Framework
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager

# ML and Data Handling
import faiss
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

# --- 2. Configuration ---
# Search API Configuration
MODEL_NAME = os.getenv("MODEL_NAME", 'allenai/scibert_scivocab_uncased')
INDEX_PATH = os.getenv("INDEX_PATH", "/dist_home/suryansh/BD/data_processing/semantic_index/s2orc_scibert_index_ivfpq.faiss")
MAPPING_PATH = os.getenv("MAPPING_PATH", "/dist_home/suryansh/BD/data_processing/semantic_index/s2orc_scibert_mapping.pkl")
LOOKUP_TABLE_PATH = os.getenv("LOOKUP_TABLE_PATH", "/dist_home/suryansh/BD/data_processing/semantic_index/s2orc_id_lookup_table_local/")
DEFAULT_NPROBE = int(os.getenv("DEFAULT_NPROBE", 64))

# Paper Fetcher API Configuration
SPARK_JOB_SCRIPT = "fetch_paper_job.py"
HDFS_ORIGINAL_DATA_PATH = "hdfs:///s2orc_paper_data_cleaned/s2orc_paper_data_cleaned.parquet"
SPARK_MASTER = "spark://asaicomputenode02.amritanet.edu:7077"
HDFS_NAMENODE = "hdfs://172.17.16.11:9000"

# --- 3. Global Variables ---
# This dictionary will hold the models and data loaded at startup.
api_globals = {}

# --- 4. Pydantic Models ---

# Models for the Search API
class SearchQuery(BaseModel):
    query_text: str = Field(..., example="Machine learning applications in healthcare")
    top_k: int = Field(10, gt=0, le=100)
    nprobe: int = Field(DEFAULT_NPROBE, gt=0, le=2048)

class SearchResult(BaseModel):
    rank: int
    title: str
    paper_id: int
    corpus_id: Optional[int] = None
    approximate_distance: float

# Models for the Paper Fetcher API
class Author(BaseModel):
    authorId: Optional[str] = None
    name: Optional[str] = None

class ExternalIDs(BaseModel):
    ACL: Optional[str] = None
    ArXiv: Optional[str] = None
    CorpusId: Optional[str] = None
    DBLP: Optional[str] = None
    DOI: Optional[str] = None
    MAG: Optional[str] = None
    PubMed: Optional[str] = None
    PubMedCentral: Optional[str] = None

class Journal(BaseModel):
    name: Optional[str] = None
    pages: Optional[str] = None
    volume: Optional[str] = None

class S2FieldOfStudy(BaseModel):
    category: Optional[str] = None
    source: Optional[str] = None

class FullPaperData(BaseModel):
    authors: Optional[list] = None
    citationcount: Optional[int] = None
    corpusid: Optional[int] = None
    externalids: Optional[dict] = None
    influentialcitationcount: Optional[int] = None
    isopenaccess: Optional[bool] = None
    journal: Optional[dict] = None
    publicationdate: Optional[str] = None
    publicationtypes: Optional[List[str]] = None
    publicationvenueid: Optional[str] = None
    referencecount: Optional[int] = None
    s2fieldsofstudy: Optional[list] = None
    title: Optional[str] = None
    url: Optional[str] = None
    venue: Optional[str] = None
    year: Optional[int] = None


# --- 5. Lifespan Event Handler (for Startup and Shutdown) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Load all necessary files and models for the Search functionality when the API server starts.
    """
    print("--- Loading models and data for the Search API ---")

    if not os.path.exists(INDEX_PATH) or not os.path.exists(MAPPING_PATH):
        print(f"Error: Index ('{INDEX_PATH}') or Mapping ('{MAPPING_PATH}') file not found.")
    else:
        print(f"Loading SentenceTransformer model: {MODEL_NAME}")
        api_globals['model'] = SentenceTransformer(MODEL_NAME)

        print(f"Loading FAISS index from: {INDEX_PATH}")
        api_globals['index'] = faiss.read_index(INDEX_PATH)

        print(f"Loading paper mapping from: {MAPPING_PATH}")
        with open(MAPPING_PATH, 'rb') as f:
            api_globals['id_and_title_map'] = pickle.load(f)

        print("--- Search models and data loaded successfully! ---")
    
    yield
    
    # This part runs on shutdown
    print("--- Shutting down and clearing resources ---")
    api_globals.clear()


# --- 6. FastAPI App Initialization ---
app = FastAPI(
    title="Scientific Paper Search and Fetch API",
    description="A combined API to search for scientific papers using SciBERT/FAISS and fetch full paper details using Spark.",
    version="2.0.0",
    lifespan=lifespan
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# --- 7. API Endpoints ---
@app.get("/")
def read_root():
    """Root endpoint to confirm the API is running."""
    return {
        "status": "ok",
        "message": "Welcome to the Scientific Paper Search and Fetch API!",
        "endpoints": {
            "search": "POST /search",
            "fetch_paper": "GET /paper/{corpus_id}"
        }
    }

@app.post("/search", response_model=List[SearchResult])
def search_papers(query: SearchQuery):
    """
    Performs a semantic search for scientific papers.
    """
    if 'model' not in api_globals or 'index' not in api_globals or 'id_and_title_map' not in api_globals:
        raise HTTPException(status_code=503, detail="Server is not ready. Search models or data files are not loaded.")

    print(f"\nReceived search request: query='{query.query_text}', top_k={query.top_k}, nprobe={query.nprobe}")

    try:
        index = api_globals['index']
        model = api_globals['model']
        id_and_title_map = api_globals['id_and_title_map']
        
        index.nprobe = query.nprobe
        
        # 1. Encode query and perform search
        query_vector = model.encode([query.query_text], normalize_embeddings=True).astype('float32')
        distances, indices = index.search(query_vector, query.top_k)

        # 2. Get initial results (paper_id, title)
        initial_results = []
        top_paper_ids = []
        for i, idx in enumerate(indices[0]):
            paper_id, title = id_and_title_map.get(idx, (-1, "Title not found"))
            initial_results.append({
                "rank": i + 1,
                "title": title,
                "paper_id": paper_id,
                "approximate_distance": float(distances[0][i])
            })
            top_paper_ids.append(paper_id)
        
        # 3. On-demand lookup to find the corpus_id
        paper_to_corpus_id_map = {}
        if os.path.exists(LOOKUP_TABLE_PATH):
            all_lookup_files = glob.glob(os.path.join(LOOKUP_TABLE_PATH, '*.parquet'))
            for file_path in all_lookup_files:
                df = pd.read_parquet(file_path, columns=['paper_id', 'corpusid'])
                filtered_df = df[df['paper_id'].isin(top_paper_ids)]
                if not filtered_df.empty:
                    paper_to_corpus_id_map.update(filtered_df.set_index('paper_id')['corpusid'].to_dict())
        else:
            print(f"Warning: Lookup table path not found at '{LOOKUP_TABLE_PATH}'. Cannot retrieve corpus_ids.")

        # 4. Enrich results with corpus_id
        final_results = []
        for res in initial_results:
            corpus_id = paper_to_corpus_id_map.get(res["paper_id"])
            final_results.append(
                SearchResult(
                    rank=res["rank"],
                    title=res["title"],
                    paper_id=res["paper_id"],
                    corpus_id=corpus_id,
                    approximate_distance=res["approximate_distance"]
                )
            )

        print(f"Returning {len(final_results)} results.")
        return final_results

    except Exception as e:
        print(f"An error occurred during search: {e}")
        raise HTTPException(status_code=500, detail=f"An internal error occurred during search: {str(e)}")


@app.get("/paper/{corpus_id}", response_model=FullPaperData)
async def get_paper_details_with_spark(corpus_id: int):
    """
    Triggers a Spark job to fetch full paper metadata from HDFS by its corpus_id.
    """
    print(f"Received request for corpus_id: {corpus_id}. Triggering Spark job.")
    
    # Define a unique local filename for the Spark job's output
    result_filename = f"/tmp/{uuid.uuid4()}.json"
    
    # Construct the spark-submit command with memory allocation
    spark_submit_cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
        "--driver-memory", "4g",
        "--executor-memory", "80g", # Note: This is a high value, adjust as needed.
        "--conf", f"spark.hadoop.fs.defaultFS={HDFS_NAMENODE}",
        SPARK_JOB_SCRIPT,
        str(corpus_id),
        HDFS_ORIGINAL_DATA_PATH,
        result_filename
    ]
    
    print(f"Executing Spark job: {' '.join(spark_submit_cmd)}")
    
    try:
        # Execute the Spark job and wait for completion
        proc = subprocess.run(spark_submit_cmd, check=True, capture_output=True, text=True, timeout=120)
        
        print(f"--- Spark Job STDOUT ---\n{proc.stdout}")
        print(f"--- Spark Job STDERR ---\n{proc.stderr}")
        
        if not os.path.exists(result_filename):
            raise HTTPException(status_code=500, detail="Spark job finished, but the result file was not created.")
            
        with open(result_filename, 'r') as f:
            paper_data = json.load(f)
            
        if not paper_data or "error" in paper_data:
             raise HTTPException(status_code=404, detail=f"Paper with corpus_id {corpus_id} not found by Spark job.")

        return FullPaperData(**paper_data)
        
    except subprocess.CalledProcessError as e:
        print(f"Spark job failed with a non-zero exit code!\nSTDOUT: {e.stdout}\nSTDERR: {e.stderr}")
        raise HTTPException(status_code=500, detail="Spark job failed to execute. Check server logs.")
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Spark job timed out after 120 seconds.")
    finally:
        # Clean up the temporary result file
        if os.path.exists(result_filename):
            os.remove(result_filename)
