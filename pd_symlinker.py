import os
import re
import json
import csv
import time
import argparse
from datetime import datetime
from collections import defaultdict
from colorama import init, Fore, Style

# Constants
DEFAULT_CATALOG_PATH = '/catalog/catalog.csv'
src_dir = '/data/torrents'
dest_dir = '/data/sorted/shows'
dest_dir_movies = '/data/sorted/movies'

# Initialize colorama
init(autoreset=True)

# Ensure necessary directories exist
# os.makedirs(src_dir, exist_ok=True)
# os.makedirs(dest_dir, exist_ok=True)
# os.makedirs(dest_dir_movies, exist_ok=True)
# os.makedirs(os.path.dirname(DEFAULT_CATALOG_PATH), exist_ok=True)

print(f"Directories ensured: {src_dir}, {dest_dir}, {dest_dir_movies}, {DEFAULT_CATALOG_PATH}")

# Utilities
def extract_year(query):
    match = re.search(r'[\(\.\s_-](\d{4})[\)\.\s_-]', query.strip())
    if match:
        year = int(match.group(1))
        if 1900 <= year <= datetime.now().year:
            return year
    return None

def extract_resolution(name):
    resolution_match = re.search(r'(\d{3,4}p)', name, re.IGNORECASE)
    if resolution_match:
        return resolution_match.group(1)
    return None

def sanitize_title(name):
    return re.sub(r'[^a-zA-Z0-9\s.]', ' ', name).strip()  # Preserve periods

def clean_filename(filename):
    filename = re.sub(r' - - ', ' - ', filename)
    filename = re.sub(r' +', ' ', filename).strip()  # Remove extra spaces
    filename = re.sub(r' -$', '', filename)  # Remove trailing dash
    return filename

# CSV reading
def read_catalog_csv(csv_path):
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

# Extract the ID based on the presence of "tmdb" or fallback to "imdb"
def extract_id(eid_string, preferred='tmdb', fallback='imdb'):
    ids = eid_string.split(', ')
    for id_str in ids:
        if preferred in id_str:
            return sanitize_title(id_str.split(f'//')[1])
    for id_str in ids:
        if fallback in id_str:
            return sanitize_title(id_str.split(f'//')[1])
    return 'unknown'

# Symlink creation
def create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, catalog_path):
    catalog_data = read_catalog_csv(catalog_path)
    print(f"Catalog data read from {catalog_path}")

    for entry in catalog_data:
        title = entry['Title']
        type_ = entry['Type']
        year = entry['Year']
        parent_title = entry['ParentTitle']
        parent_type = entry['ParentType']
        parent_year = entry['ParentYear']
        grandparent_title = entry['GrandParentTitle']
        grandparent_type = entry['GrandParentType']
        grandparent_year = entry['GrandParentYear']
        torrent_dir_name = entry['Torrent File Name']  # Treated as directory name now
        actual_title = entry['Actual Title']

        if type_ == 'movie':
            base_title = title
            base_year = year
            tmdb_id = extract_id(entry['EID']) if entry['EID'] else 'unknown'
            target_folder = os.path.join(dest_dir_movies, f"{base_title} ({base_year}) {{tmdb-{tmdb_id}}}")
        else:
            base_title = grandparent_title if grandparent_title else parent_title if parent_title else title
            base_year = grandparent_year if grandparent_year else parent_year if parent_year else year
            tmdb_id = extract_id(entry.get('GrandParentEID')) if entry.get('GrandParentEID') else extract_id(entry.get('ParentEID')) if entry.get('ParentEID') else extract_id(entry.get('EID')) if entry.get('EID') else 'unknown'
            target_folder = os.path.join(dest_dir, f"{base_title} ({base_year}) {{tmdb-{tmdb_id}}}")

        # Ensure the target folder exists
        if not os.path.exists(target_folder):
            try:
                os.makedirs(target_folder)
                print(f"Created target folder: {target_folder}")
            except OSError as e:
                print(f"Error creating target folder: {e}")
                continue

        # Process files within the torrent directory
        torrent_dir_path = os.path.join(src_dir, torrent_dir_name)
        print(f"Processing torrent directory: {torrent_dir_path}")
        time.sleep(5)  # Wait for 5 seconds

        if os.path.isdir(torrent_dir_path):
            pass
        else:
            torrent_dir_path = os.path.join(src_dir, actual_title)
        if os.path.isdir(torrent_dir_path):
            pass
        else:
            torrent_dir_path = torrent_dir_path[:-4]
            
        for file_name in os.listdir(torrent_dir_path):
            file_path = os.path.join(torrent_dir_path, file_name)
            print(f"Processing file: {file_path}")

            if os.path.isfile(file_path):
                file_ext = os.path.splitext(file_name)[1]  # Extract file extension

                if type_ == 'movie':
                    target_file_name = f"{base_title} ({base_year}) [{extract_resolution(file_name)}]{file_ext}"
                    target_folder_season = target_folder  # For movies, use the main target folder
                    target_file_path = os.path.join(target_folder, target_file_name)
                else:
                    season_number_match = re.search(r'S(\d{2})E\d{2}', file_name)
                    if season_number_match:
                        season_folder = f"Season {season_number_match.group(1)}"
                    else:
                        season_folder = "Season Unknown"

                    target_folder_season = os.path.join(target_folder, season_folder)
                    episode_identifier = re.search(r'(S\d{2}E\d{2})', file_name)
                    if episode_identifier:
                        episode_identifier = episode_identifier.group(1)
                    else:
                        episode_identifier = "Unknown Episode"
                    target_file_name = f"{base_title} ({base_year}) - {episode_identifier} [{extract_resolution(file_name)}]{file_ext}"

                    if not os.path.exists(target_folder_season):
                        os.makedirs(target_folder_season, exist_ok=True)

                    target_file_path = os.path.join(target_folder_season, target_file_name)

                target_file_name = clean_filename(target_file_name)

                if not os.path.exists(file_path):
                    print(f"Source file does not exist: {file_path}")
                elif not os.path.exists(target_file_path):
                    try:
                        # Create relative symlink
                        relative_source_path = os.path.relpath(file_path, os.path.dirname(target_file_path))
                        os.symlink(relative_source_path, target_file_path)
                        print(f"Created relative symlink: {target_file_path} -> {relative_source_path}")
                    except OSError as e:
                        print(f"Error creating relative symlink: {e}")
                else:
                    print(f"Symlink already exists: {target_file_path}")

def create_symlinks():
    print("create_symlinks function called.")
    create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, DEFAULT_CATALOG_PATH)
    print("create_symlinks function completed.")
