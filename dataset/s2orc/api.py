import requests
import os
import subprocess
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# Config
BASE_URL = "https://api.semanticscholar.org/datasets/v1/release/"
API_KEY = "<Enter your api_key"
HEADERS = {"x-api-key": API_KEY}
RELEASE_ID = "2025-08-12"
DATASET_NAME = "s2orc"
SAVE_DIR = "/dist_home/suryansh/BD/dataset/s2orc/s2orc_data"

os.makedirs(SAVE_DIR, exist_ok=True)

def get_new_links():
    """Fetch a fresh list of pre-signed URLs."""
    url = f"{BASE_URL}{RELEASE_ID}/dataset/{DATASET_NAME}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json().get("files", [])

def download_file(presigned_url):
    """Download one file from a pre-signed URL."""
    filename = unquote(os.path.basename(urlparse(presigned_url).path))
    save_path = os.path.join(SAVE_DIR, filename)

    if os.path.exists(save_path):
        print(f"Already exists: {filename}, skipping.")
        return

    print(f"Downloading: {filename}")
    result = subprocess.run(["curl", "-C", "-", "-L", "-o", save_path, presigned_url])
    if result.returncode == 0:
        print(f"Finished: {filename}")
    else:
        print(f"Failed: {filename}")

def download_in_batches(batch_size=10, workers=4):
    """Download dataset in small batches with fresh links each time."""
    index = 0
    while True:
        files_list = get_new_links()
        if index >= len(files_list):
            print("All files processed.")
            break

        batch = files_list[index:index+batch_size]
        if not batch:
            print("No more files in this batch.")
            break

        print(f"\n=== Batch {index} → {index+len(batch)-1} ===")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(download_file, u) for u in batch]
            for f in as_completed(futures):
                f.result()

        index += batch_size

# Run downloader
download_in_batches(batch_size=10, workers=4)
