import os
import re
import csv
import json
from datetime import datetime
from colorama import init
from fuzzywuzzy import fuzz, process
from moviepy.editor import VideoFileClip
import asyncio
from organisemedia import process_unaccounted_folder

# Constants
DEFAULT_CATALOG_PATH = '/catalog/catalog.csv'
PROCESSED_ITEMS_FILE = '/catalog/processed_items.txt'
SRC_DIR = os.getenv('SRC_DIR', '')
DEST_DIR = os.getenv('DEST_DIR', '')
src_dir = SRC_DIR
dest_dir = os.path.join(DEST_DIR, "shows")
dest_dir_movies = os.path.join(DEST_DIR, "movies")

# Initialize colorama
init(autoreset=True)

def extract_year(query):
    match = re.search(r'[\(\.\s_-](\d{4})[\)\.\s_-]', query.strip())
    if match:
        year = int(match.group(1))
        if 1900 <= year <= datetime.now().year:
            return year
    return None

def extract_resolution(name, parent_folder_name=None, file_path=None):
    resolution_match = re.search(r'(\d{3,4}p)', name, re.IGNORECASE)
    if resolution_match:
        return resolution_match.group(1)

    if parent_folder_name:
        resolution_match = re.search(r'(\d{3,4}p)', parent_folder_name, re.IGNORECASE)
        if resolution_match:
            return resolution_match.group(1)

    if file_path:
        try:
            clip = VideoFileClip(file_path)
            width, height = clip.size
            if width and height:
                if width in [720, 1080, 2160]:
                    return f"{width}p"
                else:
                    return f"{width}x{height}"
        except Exception as e:
            print(f"Error getting resolution with MoviePy: {e}")
        return None
    return None

def sanitize_title(name):
    return re.sub(r'[^a-zA-Z0-9\s]', ' ', name).strip()  # Don't Preserve periods

def clean_filename(filename):
    filename = re.sub(r' - - ', ' - ', filename)
    filename = re.sub(r' +', ' ', filename).strip()  # Remove extra spaces
    filename = re.sub(r' -$', '', filename)  # Remove trailing dash
    return filename

def read_catalog_csv(csv_path):
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            return list(reader)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

def extract_id(eid_string, preferred='tmdb', fallback='imdb'):
    ids = eid_string.split(', ')
    for id_str in ids:
        if preferred in id_str:
            return sanitize_title(id_str.split(f'//')[1])
    for id_str in ids:
        if fallback in id_str:
            return sanitize_title(id_str.split(f'//')[1])
    return 'unknown'

def find_best_match(torrent_dir_name, actual_title, src_dir):
    try:
        # Get all directories in the source directory
        dirs = os.listdir(src_dir)
        
        # Sanitize the directory names and create a mapping to the original names
        sanitized_dirs = {sanitize_title(d): d for d in dirs}

        # Function to find the largest file in a directory
        def get_largest_file(directory):
            largest_file = None
            largest_size = 0
            for file_name in os.listdir(directory):
                file_path = os.path.join(directory, file_name)
                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    if file_size > largest_size:
                        largest_size = file_size
                        largest_file = file_name
            return largest_file

        # Matching with unsanitized and sanitized names
        attempts = [
            (torrent_dir_name, dirs),
            (actual_title, dirs),
            (sanitize_title(torrent_dir_name), dirs),
            (sanitize_title(actual_title), dirs),
            (sanitize_title(torrent_dir_name), sanitized_dirs.keys()),
            (sanitize_title(actual_title), sanitized_dirs.keys())
        ]

        for query, candidates in attempts:
            best_match, score = process.extractOne(query, candidates, scorer=fuzz.ratio)
            if score >= 90:
                return os.path.join(src_dir, sanitized_dirs.get(best_match, best_match))

        # If no match is found, check within directories based on the largest file
        for directory in dirs:
            dir_path = os.path.join(src_dir, directory)
            if os.path.isdir(dir_path):
                largest_file = get_largest_file(dir_path)
                if largest_file:
                    best_match, score = process.extractOne(torrent_dir_name, [largest_file], scorer=fuzz.ratio)
                    if score >= 90:
                        return dir_path

                    best_match, score = process.extractOne(actual_title, [largest_file], scorer=fuzz.ratio)
                    if score >= 90:
                        return dir_path

    except Exception as e:
        print(f"Error finding best match: {e}")
    return None

def read_processed_items(file_path):
    if not os.path.exists(file_path):
        return set()
    with open(file_path, 'r') as file:
        processed_items = {line.strip() for line in file.readlines()}
    return processed_items

def write_processed_items(file_path, processed_items):
    with open(file_path, 'w') as file:
        file.write('\n'.join(processed_items))

def extract_season_episode(file_name):
    # List of regex patterns to match different season and episode naming conventions
    patterns = [
        r'[Ss](\d{1,2})[Ee](\d{1,2})',       # S01E02 or s01e02
        r'(\d{1,2})[xX](\d{1,2})',            # 1x02 or 1X02
        r'[Ss]eason\s*(\d{1,2})\s*[Ee]pisode\s*(\d{1,2})',  # Season 1 Episode 2
        r'[Ss](\d{1,2})\.[Ee](\d{1,2})',      # S01.E02
        r'[Ee]p?\s*(\d{1,2})',                # Ep02, Ep 02, E02
    ]

    for pattern in patterns:
        match = re.search(pattern, file_name, re.IGNORECASE)
        if match:
            season = match.group(1).zfill(2)
            episode = match.group(2).zfill(2) if len(match.groups()) > 1 else None
            return season, episode
    return None, None

async def handle_unaccounted_directory(directory, dest_dir):
    # Attempt to find best match
    match = find_best_match(directory, directory, src_dir)
    if match:
        print(f"Found best match for {directory}: {match}")
    else:
        print(f"No match found for {directory}. Processing with organisemedia logic.")
        await process_unaccounted_folder(os.path.join(src_dir, directory), dest_dir)

def create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, catalog_path, processed_items_file):
    catalog_data = read_catalog_csv(catalog_path)
    processed_items = read_processed_items(processed_items_file)
    new_processed_items = set(processed_items)

    print(f"Catalog data read from {catalog_path}")

    # Get all directories in the source directory
    all_dirs = set(os.listdir(src_dir))

    # Get all torrent directories listed in the catalog
    catalog_dirs = set(entry['Torrent File Name'] for entry in catalog_data)

    # Identify unaccounted directories
    unaccounted_dirs = all_dirs - catalog_dirs - processed_items

    for unaccounted_dir in unaccounted_dirs:
        print(f"Processing unaccounted directory: {unaccounted_dir}")
        asyncio.run(handle_unaccounted_directory(unaccounted_dir, dest_dir_movies))
        new_processed_items.add(unaccounted_dir)

    for entry in catalog_data:
        try:
            eid = entry['EID']
            torrent_dir_name = entry['Torrent File Name']
            
            if torrent_dir_name in processed_items:
                print("Already Processed")
                continue

            title = entry['Title']
            type_ = entry['Type']
            year = entry['Year']
            parent_title = entry['ParentTitle']
            parent_type = entry['ParentType']
            parent_year = entry['ParentYear']
            grandparent_title = entry['GrandParentTitle']
            grandparent_type = entry['GrandParentType']
            grandparent_year = entry['GrandParentYear']
            actual_title = entry['Actual Title']

            if type_ == 'movie':
                # Movie handling code
                base_title = title
                base_year = year
                tmdb_id = extract_id(entry['EID']) if entry['EID'] else 'unknown'
                if f"({base_year})" in base_title:
                    folder_name = f"{base_title} {{tmdb-{tmdb_id}}}"
                else:
                    folder_name = f"{base_title} ({base_year}) {{tmdb-{tmdb_id}}}"
                target_folder = os.path.join(dest_dir_movies, folder_name)

                # Ensure target folder exists
                if not os.path.exists(target_folder):
                    os.makedirs(target_folder, exist_ok=True)
                    print(f"Created target folder: {target_folder}")

                torrent_dir_path = find_best_match(torrent_dir_name, actual_title, src_dir)
                if not torrent_dir_path:
                    print(f"No matching directory found for {torrent_dir_name} or {actual_title}. Processing with organisemedia logic.")
                    asyncio.run(process_unaccounted_folder(os.path.join(src_dir, torrent_dir_name), dest_dir_movies))
                    continue
                print(f"Processing torrent directory: {torrent_dir_path}")

                # Find the largest file in the movie folder
                largest_file = None
                largest_size = 0
                for file_name in os.listdir(torrent_dir_path):
                    file_path = os.path.join(torrent_dir_path, file_name)
                    if os.path.isfile(file_path):
                        file_size = os.path.getsize(file_path)
                        if file_size > largest_size:
                            largest_size = file_size
                            largest_file = file_name

                if largest_file:
                    file_ext = os.path.splitext(largest_file)[1]
                    resolution = extract_resolution(largest_file, parent_folder_name=torrent_dir_path, file_path=os.path.join(torrent_dir_path, largest_file))
                    target_file_name = f"{base_title}  ({base_year}) {{tmdb-{tmdb_id}}} [{resolution}]{file_ext}"
                    target_file_name = clean_filename(target_file_name)
                    target_file_path = os.path.join(target_folder, target_file_name)
                    
                    largest_file_path = os.path.join(torrent_dir_path, largest_file)
                    if not os.path.exists(target_file_path):
                        try:
                            # Create relative symlink
                            relative_source_path = os.path.relpath(largest_file_path, os.path.dirname(target_file_path))
                            os.symlink(relative_source_path, target_file_path)
                            print(f"Created relative symlink: {target_file_path} -> {relative_source_path}")
                        except OSError as e:
                            print(f"Error creating relative symlink: {e}")
                    else:
                        print(f"Symlink already exists: {target_file_path}")

            else:
                # TV show handling code
                base_title = grandparent_title if grandparent_title else parent_title if parent_title else title
                base_year = grandparent_year if grandparent_year else parent_year if parent_year else year
                tmdb_id = extract_id(entry.get('GrandParentEID')) if entry.get('GrandParentEID') else extract_id(entry.get('ParentEID')) if entry.get('ParentEID') else extract_id(entry.get('EID')) if entry.get('EID') else 'unknown'

                if f"({base_year})" in base_title:
                    folder_name = f"{base_title} {{tmdb-{tmdb_id}}}"
                else:
                    folder_name = f"{base_title} ({base_year}) {{tmdb-{tmdb_id}}}"
                target_folder = os.path.join(dest_dir, folder_name)

                if not os.path.exists(target_folder):
                    try:
                        os.makedirs(target_folder)
                        print(f"Created target folder: {target_folder}")
                    except OSError as e:
                        print(f"Error creating target folder: {e}")
                        continue

                torrent_dir_path = find_best_match(torrent_dir_name, actual_title, src_dir)
                print(torrent_dir_path)
                if not torrent_dir_path:
                    print(f"No matching directory found for {torrent_dir_name} or {actual_title}. Processing with organisemedia logic.")
                    asyncio.run(process_unaccounted_folder(os.path.join(src_dir, torrent_dir_name), dest_dir))
                    continue
                print(f"Processing torrent directory: {torrent_dir_path}")

                for file_name in os.listdir(torrent_dir_path):
                    file_path = os.path.join(torrent_dir_path, file_name)
                    print(f"Processing file: {file_path}")

                    if os.path.isfile(file_path):
                        file_ext = os.path.splitext(file_name)[1]

                        season, episode = extract_season_episode(file_name)
                        if not (season and episode):
                            print(f"Skipping file (no season/episode info): {file_name}")
                            continue

                        season_folder = f"Season {season}"
                        episode_identifier = f"S{season}E{episode}"

                        # Check if a symlink with any resolution already exists
                        target_folder_season = os.path.join(target_folder, season_folder)
                        existing_files = os.listdir(target_folder_season) if os.path.exists(target_folder_season) else []
                        episode_pattern = f"{base_title} ({base_year}) {{tmdb-{tmdb_id}}} - {episode_identifier} ["
                        if any(f.startswith(episode_pattern) and f.endswith(file_ext) for f in existing_files):
                            print(f"Symlink for {episode_identifier} already exists. Skipping file: {file_name}")
                            continue

                        resolution = extract_resolution(file_name, parent_folder_name=torrent_dir_path, file_path=file_path)
                        target_file_name = f"{base_title} ({base_year}) {{tmdb-{tmdb_id}}} - {episode_identifier} [{resolution}]{file_ext}"

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

            new_processed_items.add(torrent_dir_name)
        except Exception as e:
            print(f"Error processing entry: {e}")

    write_processed_items(processed_items_file, new_processed_items)

def create_symlinks():
    print("create_symlinks function called.")
    try:
        create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, DEFAULT_CATALOG_PATH, PROCESSED_ITEMS_FILE)
    except Exception as e:
        print(f"Error in create_symlinks: {e}")
    print("create_symlinks function completed.")
