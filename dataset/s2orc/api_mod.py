import requests
import os
import subprocess
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# Config
BASE_URL = "https://api.semanticscholar.org/datasets/v1/release/"
API_KEY = "<Enter your api key>"
HEADERS = {"x-api-key": API_KEY}
RELEASE_ID = "2025-08-12"
DATASET_NAME = "s2orc"
SAVE_DIR = "/dist_home/suryansh/BD/dataset/s2orc/s2orc_data_failed"

os.makedirs(SAVE_DIR, exist_ok=True)

# List of failed filenames (just the .gz files)
FAILED_FILES = [
	"20250815_113147_00039_6wjwx_795a8455-11d8-46de-a5c8-7c96bf6c3c98.gz",
	"20250815_113147_00039_6wjwx_d9eb6856-ae70-4c7a-8610-0e70e151cbd6.gz",
]

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

    if filename not in FAILED_FILES:
        return  # skip files not in failed list

    if os.path.exists(save_path):
        print(f"Already exists: {filename}, skipping.")
        return

    print(f"Downloading: {filename}")
    result = subprocess.run(["curl", "-C", "-", "-L", "-o", save_path, presigned_url])
    if result.returncode == 0:
        print(f"Finished: {filename}")
    else:
        print(f"Failed again: {filename}")

def redownload_failed(workers=4):
    """Only retry downloading failed files with fresh URLs."""
    files_list = get_new_links()
    target_urls = [u for u in files_list if any(f in u for f in FAILED_FILES)]

    print(f"Retrying {len(target_urls)} failed files...")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(download_file, u) for u in target_urls]
        for f in as_completed(futures):
            f.result()

# Run retry
redownload_failed(workers=4)
