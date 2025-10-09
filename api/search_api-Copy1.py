#backend.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import subprocess
import json
import uuid
from typing import List, Optional

# --- 1. Configuration ---
# Path to the Spark job script that this API will execute
SPARK_JOB_SCRIPT = "fetch_paper_job.py" 

# HDFS path to the full dataset that the Spark job will read from
HDFS_ORIGINAL_DATA_PATH = "hdfs:///s2orc_paper_data_cleaned/s2orc_paper_data_cleaned.parquet"

# Connection details for your Spark cluster
SPARK_MASTER = "spark://asaicomputenode02.amritanet.edu:7077"
HDFS_NAMENODE = "hdfs://172.17.16.11:9000"

# --- 2. Pydantic Models for the Full Paper Schema ---
# These models define the structure of the JSON response.
class Author(BaseModel):
    authorId: Optional[str] = None
    name: Optional[str] = None

class ExternalIDs(BaseModel):
    ACL: Optional[str] = None; ArXiv: Optional[str] = None; CorpusId: Optional[str] = None; DBLP: Optional[str] = None; DOI: Optional[str] = None; MAG: Optional[str] = None; PubMed: Optional[str] = None; PubMedCentral: Optional[str] = None

class Journal(BaseModel):
    name: Optional[str] = None; pages: Optional[str] = None; volume: Optional[str] = None

class S2FieldOfStudy(BaseModel):
    category: Optional[str] = None; source: Optional[str] = None

class FullPaperData(BaseModel):
    authors: Optional[list] = None; citationcount: Optional[int] = None; corpusid: Optional[int] = None; externalids: Optional[dict] = None; influentialcitationcount: Optional[int] = None; isopenaccess: Optional[bool] = None; journal: Optional[dict] = None; publicationdate: Optional[str] = None; publicationtypes: Optional[List[str]] = None; publicationvenueid: Optional[str] = None; referencecount: Optional[int] = None; s2fieldsofstudy: Optional[list] = None; title: Optional[str] = None; url: Optional[str] = None; venue: Optional[str] = None; year: Optional[int] = None

# --- 3. FastAPI App ---
app = FastAPI()

# --- 4. API Endpoint ---
@app.get("/paper/{corpus_id}", response_model=FullPaperData)
async def get_paper_details_with_spark(corpus_id: int):
    """
    Receives a request for a corpus_id and triggers a Spark job
    to fetch the full metadata directly from HDFS.
    """
    print(f"Received request for corpus_id: {corpus_id}. Triggering Spark job.")
    
    # Define a unique local filename on the server for the Spark job's output
    result_filename = f"/tmp/{uuid.uuid4()}.json"
    
    # Add memory allocation flags to the spark-submit command
    # This prevents the "Java heap space" OutOfMemoryError.
    spark_submit_cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
        "--driver-memory", "4g",
        "--executor-memory", "80g",
        "--conf", f"spark.hadoop.fs.defaultFS={HDFS_NAMENODE}",
        SPARK_JOB_SCRIPT,
        str(corpus_id),
        HDFS_ORIGINAL_DATA_PATH,
        result_filename
    ]
    
    print(f"Executing Spark job: {' '.join(spark_submit_cmd)}")
    
    try:
        # Execute the Spark job and wait for it to complete.
        proc = subprocess.run(spark_submit_cmd, check=True, capture_output=True, text=True, timeout=120)
        
        # Improved Logging: Always print the output from the Spark job for debugging
        print(f"--- Spark Job STDOUT ---\n{proc.stdout}")
        print(f"--- Spark Job STDERR ---\n{proc.stderr}")
        
        if not os.path.exists(result_filename):
            raise HTTPException(status_code=500, detail="Spark job finished, but the result file was not created.")
            
        # **FIXED**: Correctly assign the file handle to the variable 'f'
        with open(result_filename, 'r') as f:
            paper_data = json.load(f)
            
        if not paper_data or "error" in paper_data:
             raise HTTPException(status_code=404, detail=f"Paper with corpus_id {corpus_id} not found by Spark job. See API server logs for details.")

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

@app.get("/")
def read_root():
    return {"message": "Paper Fetcher API is running. Make a GET request to /paper/{corpus_id}."}

