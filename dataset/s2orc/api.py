from urllib.parse import urlparse, unquote
import requests
import json
import subprocess
import os

base_url = "https://api.semanticscholar.org/datasets/v1/release/"

save_dir = "/dist_home/suryansh/BD/dataset/s2orc/s2orc_dataset"

# Make sure the directory exists
os.makedirs(save_dir, exist_ok=True)

# This endpoint requires authentication via api key
api_key = "<Enter your api key>"
headers = {"x-api-key": api_key}

# Set the release id
release_id = "2025-07-22"

# Define dataset name you want to download
dataset_name = 's2orc'

# Send the GET request and store the response in a variable
response = requests.get(base_url + release_id + '/dataset/' + dataset_name, headers=headers)
files_list = response.json().get("files")
# Download each file to the desired path
for url in files_list:
    # Parse the URL and strip query parameters
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    filename = unquote(filename)  # decode %2F etc.

    save_path = os.path.join(save_dir, filename)
    print(f"Downloading: {url} to {save_path}")

    subprocess.run(["curl", "-L", "-o", save_path, url])
