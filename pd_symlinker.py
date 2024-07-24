import os
import re
import csv
import subprocess
import json
from datetime import datetime
from colorama import init
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import imageio_ffmpeg as ffmpeg

# Constants
DEFAULT_CATALOG_PATH = '/catalog/catalog.csv'
PROCESSED_ITEMS_FILE = '/catalog/processed_items.txt'
src_dir = '/Zurg_Stranula/pd_zurg_mnt_stranula/torrents'
dest_dir = '/Zurg_Stranula/sorted/shows'
dest_dir_movies = '/Zurg_Stranula/sorted/movies'

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
            ffprobe_path = ffmpeg.get_ffmpeg_exe()  # Get the path to the bundled ffprobe
            result = subprocess.run(
                [ffprobe_path, "-v", "error", "-select_streams", "v:0", 
                 "-show_entries", "stream=width,height", "-of", "json", file_path],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                probe_data = json.loads(result.stdout)
                video_stream = probe_data['streams'][0]
                width = video_stream['width']
                height = video_stream['height']
                if width in [720, 1080, 2160]:
                    return f"{width}p"
                else:
                    return f"{width}x{height}"
            else:
                print(f"Error: {result.stderr}")
                return None
        except Exception as e:
            print(f"Error getting resolution with ffprobe: {e}")
            return None
        except Exception as e:
            print(f"Error getting resolution: {e}")
            return None
    return None

def sanitize_title(name):
    return re.sub(r'[^a-zA-Z0-9\s.]', ' ', name).strip()  # Preserve periods

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
        dirs = os.listdir(src_dir)
        best_match, score = process.extractOne(torrent_dir_name, dirs, scorer=fuzz.ratio)
        if score >= 90:
            return os.path.join(src_dir, best_match)
        best_match, score = process.extractOne(actual_title, dirs, scorer=fuzz.ratio)
        if score >= 90:
            return os.path.join(src_dir, best_match)
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

def create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, catalog_path, processed_items_file):
    catalog_data = read_catalog_csv(catalog_path)
    processed_items = read_processed_items(processed_items_file)
    new_processed_items = set(processed_items)

    print(f"Catalog data read from {catalog_path}")

    for entry in catalog_data:
        try:
            eid = entry['EID']
            if eid in processed_items:
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
            torrent_dir_name = entry['Torrent File Name']
            actual_title = entry['Actual Title']

            if type_ == 'movie':
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
                    print(f"No matching directory found for {torrent_dir_name} or {actual_title}")
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
                    target_file_name = f"{base_title} ({base_year}) [{resolution}]{file_ext}"
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
                base_title = grandparent_title if grandparent_title else parent_title if parent_title else title
                base_year = grandparent_year if grandparent_year else parent_year if parent_year else year
                tmdb_id = extract_id(entry.get('GrandParentEID')) if entry.get('GrandParentEID') else extract_id(entry.get('ParentEID')) if entry.get('ParentEID') else extract_id(entry.get('EID')) if entry.get('EID') else 'unknown'

            # Avoid duplicate years in the folder name
            if f"({base_year})" in base_title:
                folder_name = f"{base_title} {{tmdb-{tmdb_id}}}"
            else:
                folder_name = f"{base_title} ({base_year}) {{tmdb-{tmdb_id}}}"

            if type_ == 'movie':
                target_folder = os.path.join(dest_dir_movies, folder_name)
            else:
                target_folder = os.path.join(dest_dir, folder_name)

            if not os.path.exists(target_folder):
                try:
                    os.makedirs(target_folder)
                    print(f"Created target folder: {target_folder}")
                except OSError as e:
                    print(f"Error creating target folder: {e}")
                    continue

            torrent_dir_path = find_best_match(torrent_dir_name, actual_title, src_dir)
            if not torrent_dir_path:
                print(f"No matching directory found for {torrent_dir_name} or {actual_title}")
                continue
            print(f"Processing torrent directory: {torrent_dir_path}")

            for file_name in os.listdir(torrent_dir_path):
                file_path = os.path.join(torrent_dir_path, file_name)
                print(f"Processing file: {file_path}")

                if os.path.isfile(file_path):
                    file_ext = os.path.splitext(file_name)[1]

                    if type_ == 'movie':
                        resolution = extract_resolution(file_name, parent_folder_name=torrent_dir_path, file_path=file_path)
                        target_file_name = f"{base_title} ({base_year}) [{resolution}]{file_ext}"
                        target_folder_season = target_folder
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
                        resolution = extract_resolution(file_name, parent_folder_name=torrent_dir_path, file_path=file_path)
                        target_file_name = f"{base_title} ({base_year}) - {episode_identifier} [{resolution}]{file_ext}"

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

            new_processed_items.add(eid)
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
