import sys
import json
from pyspark.sql import SparkSession
# **NEW**: Import LongType for casting
from pyspark.sql.functions import col, lit
from pyspark.sql.types import LongType

def fetch_paper_data(spark, corpus_id, data_path, output_path):
    """
    Finds a single paper in an HDFS Parquet dataset and saves its metadata
    as a JSON file to a local path on the cluster.
    """
    try:
        print(f"Reading Parquet data from {data_path}...")
        df = spark.read.parquet(data_path)
        print("Data loaded. Filtering for corpus_id...")
        
        # **FIX**: Cast the incoming corpus_id to LongType to ensure a perfect match with the schema.
        paper_data_row = df.filter(col("corpusid") == lit(corpus_id).cast(LongType())).first()
        
        if paper_data_row:
            paper_dict = paper_data_row.asDict(recursive=True)
            with open(output_path, 'w') as f:
                json.dump(paper_dict, f)
            print(f"SUCCESS: Wrote data for corpus_id {corpus_id} to {output_path}")
        else:
            # This message will now be visible in the API logs.
            print(f"INFO: Paper with corpus_id {corpus_id} was not found in the dataset.")
            with open(output_path, 'w') as f:
                json.dump({}, f)

    except Exception as e:
        print(f"ERROR: An exception occurred in the Spark job: {e}")
        with open(output_path, 'w') as f:
            json.dump({"error": str(e)}, f)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: fetch_paper_job.py <corpus_id> <hdfs_data_path> <local_output_path>")
        sys.exit(-1)
        
    corpus_id_arg = int(sys.argv[1])
    data_path_arg = sys.argv[2]
    output_path_arg = sys.argv[3]
    
    spark_session = SparkSession.builder \
        .appName(f"Fetch Paper Metadata for ID {corpus_id_arg}") \
        .getOrCreate()
        
    fetch_paper_data(spark_session, corpus_id_arg, data_path_arg, output_path_arg)
    
    spark_session.stop()

