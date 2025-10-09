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

# Paper Fetcher & Analytics API Configuration
SPARK_JOB_SCRIPT = "fetch_paper_job.py"
HDFS_ORIGINAL_DATA_PATH = "hdfs:///s2orc_paper_data_cleaned/s2orc_paper_data_cleaned.parquet"
SPARK_MASTER = "spark://asaicomputenode02.amritanet.edu:7077"
HDFS_NAMENODE = "hdfs://172.17.16.11:9000"
ANALYTICS_DATA_PATH = os.getenv("ANALYTICS_DATA_PATH", "/dist_home/suryansh/BD/data_processing/sciVal_like_features/s2orc_percentile_scores_local/")


# --- 3. Global Variables ---
api_globals = {}

# --- 4. Pydantic Models ---

# Models for the Search API
# **MODIFIED**: Removed 'top_k' from the request model.
class SearchQuery(BaseModel):
    query_text: str = Field(..., example="Machine learning applications in healthcare")
    nprobe: int = Field(DEFAULT_NPROBE, gt=0, le=2048)

class SearchResult(BaseModel):
    rank: int
    title: str
    paper_id: int
    corpus_id: Optional[int] = None
    approximate_distance: float

# Models for the Analytics Enrichment API
class EnrichRequest(BaseModel):
    corpus_ids: List[int]

# The response model now includes the field_id
class PaperAnalytics(BaseModel):
    corpus_id: int
    field_id: Optional[int] = None  # The Field-Weighted Citation Index (cluster ID)
    fwci: Optional[float] = None
    citation_percentile: Optional[float] = None

# Models for the Paper Fetcher API
class Author(BaseModel): authorId: Optional[str] = None; name: Optional[str] = None
class ExternalIDs(BaseModel): ACL: Optional[str] = None; ArXiv: Optional[str] = None; CorpusId: Optional[str] = None; DBLP: Optional[str] = None; DOI: Optional[str] = None; MAG: Optional[str] = None; PubMed: Optional[str] = None; PubMedCentral: Optional[str] = None
class Journal(BaseModel): name: Optional[str] = None; pages: Optional[str] = None; volume: Optional[str] = None
class S2FieldOfStudy(BaseModel): category: Optional[str] = None; source: Optional[str] = None
class FullPaperData(BaseModel): authors: Optional[list] = None; citationcount: Optional[int] = None; corpusid: Optional[int] = None; externalids: Optional[dict] = None; influentialcitationcount: Optional[int] = None; isopenaccess: Optional[bool] = None; journal: Optional[dict] = None; publicationdate: Optional[str] = None; publicationtypes: Optional[List[str]] = None; publicationvenueid: Optional[str] = None; referencecount: Optional[int] = None; s2fieldsofstudy: Optional[list] = None; title: Optional[str] = None; url: Optional[str] = None; venue: Optional[str] = None; year: Optional[int] = None

# --- 5. Lifespan Event Handler ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- Loading models and data ---")

    # Load Search components if they exist
    if os.path.exists(INDEX_PATH) and os.path.exists(MAPPING_PATH):
        print(f"Loading SentenceTransformer model: {MODEL_NAME}")
        api_globals['model'] = SentenceTransformer(MODEL_NAME)
        print(f"Loading FAISS index from: {INDEX_PATH}")
        api_globals['index'] = faiss.read_index(INDEX_PATH)
        print(f"Loading paper mapping from: {MAPPING_PATH}")
        with open(MAPPING_PATH, 'rb') as f:
            api_globals['id_and_title_map'] = pickle.load(f)
        print("--- Search components loaded successfully! ---")
    else:
        print("--- WARNING: Search component files not found. /search endpoint will be disabled. ---")
    
    # Load Analytics components if they exist
    if os.path.exists(ANALYTICS_DATA_PATH):
        print(f"Loading analytics data from: {ANALYTICS_DATA_PATH}")
        # Load the analytics data into a pandas DataFrame for fast lookups
        api_globals['analytics_df'] = pd.read_parquet(ANALYTICS_DATA_PATH).set_index('corpusid')
        print("--- Analytics data loaded successfully! ---")
    else:
        print(f"--- WARNING: Analytics data not found at '{ANALYTICS_DATA_PATH}'. /enrich endpoint will be disabled. ---")
    
    yield
    
    print("--- Shutting down and clearing resources ---")
    api_globals.clear()

# --- 6. FastAPI App Initialization ---
app = FastAPI(
    title="Scientific Paper API",
    description="Search, Fetch, and Enrich scientific paper data.",
    version="3.0.0",
    lifespan=lifespan
)

# Add CORS middleware to allow cross-origin requests (e.g., from a web UI)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)


# --- 7. API Endpoints ---
@app.get("/")
def read_root():
    return {
        "status": "ok",
        "message": "Welcome to the Scientific Paper API!",
        "endpoints": {
            "search": "POST /search",
            "fetch_paper": "GET /paper/{corpus_id}",
            "enrich": "POST /enrich"
        }
    }

@app.post("/search", response_model=List[SearchResult])
def search_papers(query: SearchQuery):
    if 'model' not in api_globals:
        raise HTTPException(status_code=503, detail="Server is not ready. Search components not loaded.")

    try:
        index = api_globals['index']; model = api_globals['model']; id_and_title_map = api_globals['id_and_title_map']
        index.nprobe = query.nprobe
        
        # **MODIFIED**: 'top_k' is now hardcoded to 1000.
        top_k = 1000
        
        query_vector = model.encode([query.query_text], normalize_embeddings=True).astype('float32')
        distances, indices = index.search(query_vector, top_k)

        initial_results = []; top_paper_ids = []
        for i, idx in enumerate(indices[0]):
            paper_id, title = id_and_title_map.get(idx, (-1, "Title not found"))
            initial_results.append({"rank": i + 1, "title": title, "paper_id": paper_id, "approximate_distance": float(distances[0][i])})
            top_paper_ids.append(paper_id)
        
        paper_to_corpus_id_map = {}
        if os.path.exists(LOOKUP_TABLE_PATH):
            all_lookup_files = glob.glob(os.path.join(LOOKUP_TABLE_PATH, '*.parquet'))
            for file_path in all_lookup_files:
                df = pd.read_parquet(file_path, columns=['paper_id', 'corpusid'])
                filtered_df = df[df['paper_id'].isin(top_paper_ids)]
                if not filtered_df.empty:
                    paper_to_corpus_id_map.update(filtered_df.set_index('paper_id')['corpusid'].to_dict())

        final_results = []
        for res in initial_results:
            corpus_id = paper_to_corpus_id_map.get(res["paper_id"])
            final_results.append(SearchResult(rank=res["rank"], title=res["title"], paper_id=res["paper_id"], corpus_id=corpus_id, approximate_distance=res["approximate_distance"]))

        return final_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An internal error occurred during search: {str(e)}")

@app.get("/paper/{corpus_id}", response_model=FullPaperData)
async def get_paper_details_with_spark(corpus_id: int):
    result_filename = f"/tmp/{uuid.uuid4()}.json"
    spark_submit_cmd = [
        "spark-submit", "--master", SPARK_MASTER,
        "--driver-memory", "4g", "--executor-memory", "80g",
        "--conf", f"spark.hadoop.fs.defaultFS={HDFS_NAMENODE}",
        SPARK_JOB_SCRIPT, str(corpus_id), HDFS_ORIGINAL_DATA_PATH, result_filename
    ]
    
    print(f"Executing Spark job: {' '.join(spark_submit_cmd)}")
    try:
        proc = subprocess.run(spark_submit_cmd, check=True, capture_output=True, text=True, timeout=120)
        print(f"--- Spark Job STDOUT ---\n{proc.stdout}\n--- Spark Job STDERR ---\n{proc.stderr}")
        
        if not os.path.exists(result_filename):
            raise HTTPException(status_code=500, detail="Spark job finished, but result file was not created.")
            
        with open(result_filename, 'r') as f:
            paper_data = json.load(f)
            
        if not paper_data or "error" in paper_data:
                raise HTTPException(status_code=404, detail=f"Paper with corpus_id {corpus_id} not found by Spark job.")

        return FullPaperData(**paper_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Spark job failed: {str(e)}")
    finally:
        if os.path.exists(result_filename):
            os.remove(result_filename)

@app.post("/enrich", response_model=List[PaperAnalytics])
def enrich_papers_with_analytics(request: EnrichRequest):
    """
    Accepts a list of corpus_ids and returns their pre-computed FWCI and
    Top Citation Percentile.
    """
    if 'analytics_df' not in api_globals:
        raise HTTPException(status_code=503, detail="Server is not ready. Analytics data is not loaded.")

    analytics_df = api_globals['analytics_df']
    results = []

    print(f"Enriching data for {len(request.corpus_ids)} corpus_ids...")
    for corpus_id in request.corpus_ids:
        # Use .loc to find all rows matching the index (corpus_id)
        analytics_row = analytics_df.loc[analytics_df.index == corpus_id]
        
        if not analytics_row.empty:
            analytics = analytics_row.iloc[0]
            # Convert rank (0.0=top, 1.0=bottom) to percentile (100=top, 0=bottom)
            percentile = (1 - analytics['percentile_rank']) * 100
            results.append(PaperAnalytics(
                corpus_id=corpus_id,
                # Add the field_id to the response
                field_id=int(analytics['field_id']),
                fwci=analytics['fwci'],
                citation_percentile=percentile
            ))
        else:
            # If the ID has no analytics, return it with null values
            results.append(PaperAnalytics(corpus_id=corpus_id))
    
    return results