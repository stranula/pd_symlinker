import os
import requests
from zipfile import ZipFile

FFPROBE_URL = 'https://example.com/path/to/ffprobe.zip'  # Replace with the actual URL
FFPROBE_DIR = './ffprobe'
FFPROBE_PATH = os.path.join(FFPROBE_DIR, 'ffprobe')

def download_ffprobe():
    if not os.path.exists(FFPROBE_PATH):
        os.makedirs(FFPROBE_DIR, exist_ok=True)
        response = requests.get(FFPROBE_URL)
        zip_path = os.path.join(FFPROBE_DIR, 'ffprobe.zip')
        with open(zip_path, 'wb') as file:
            file.write(response.content)

        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(FFPROBE_DIR)
        
        os.remove(zip_path)
        print(f"Downloaded and extracted ffprobe to {FFPROBE_PATH}")
    else:
        print(f"ffprobe already exists at {FFPROBE_PATH}")

# Ensure ffprobe is downloaded before using it
download_ffprobe()
