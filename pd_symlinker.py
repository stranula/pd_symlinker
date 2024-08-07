import sqlite3
import os
import re
import subprocess
import json
from datetime import datetime
from colorama import init
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from moviepy.editor import VideoFileClip
import asyncio
from organisemedia import process_unaccounted_folder
import threading

# Constants
DEFAULT_CATALOG_PATH = '/data/catalog.csv'
PROCESSED_ITEMS_FILE = '/data/processed_items.txt'
SRC_DIR = os.getenv('SRC_DIR', '')
DEST_DIR = os.getenv('DEST_DIR', '')
src_dir = SRC_DIR
dest_dir = os.path.join(DEST_DIR, "shows")
dest_dir_movies = os.path.join(DEST_DIR, "movies")
DATABASE_PATH = '/data/media_database.db'
db_lock = threading.Lock()

# Initialize colorama
init(autoreset=True)

def read_catalog_db():
    with db_lock:
        print(DATABASE_PATH)
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('SELECT * FROM catalog')
        rows = c.fetchall()
        conn.close()
        return rows

def update_catalog_entry(processed_dir_name, final_symlink_path, torrent_file_name):
    with db_lock:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''
            UPDATE catalog
            SET processed_dir_name = ?, final_symlink_path = ?
            WHERE torrent_file_name = ?
        ''', (processed_dir_name, final_symlink_path, torrent_file_name))
        conn.commit()
        conn.close()

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

def extract_id(eid_string, preferred='imdb', fallback='tmdb'):
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
        sanitized_dirs = {sanitize_title(d): d for d in dirs}

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

def extract_season_episode(file_name):
    patterns = [
        r'[Ss](\d{1,2})[Ee](\d{1,2})',
        r'(\d{1,2})[xX](\d{1,2})',
        r'[Ss]eason\s*(\d{1,2})\s*[Ee]pisode\s*(\d{1,2})',
        r'[Ss](\d{1,2})\.[Ee](\d{1,2})',
        r'[Ee]p?\s*(\d{1,2})',
    ]

    for pattern in patterns:
        match = re.search(pattern, file_name, re.IGNORECASE)
        if match:
            season = match.group(1).zfill(2)
            episode = match.group(2).zfill(2) if len(match.groups()) > 1 else None
            return season, episode
    return None, None

def create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, catalog_path):
    catalog_data = read_catalog_db()
    processed_items = {entry[5] for entry in catalog_data if entry[5]}

    for entry in catalog_data:
        try:
            eid = entry[1]
            torrent_dir_name = entry[13]
            actual_title_name = entry[14]

            torrent_dir_path = find_best_match(torrent_dir_name, actual_title_name, src_dir)
            if not torrent_dir_path:
                continue

            if torrent_dir_path in processed_items:
                continue

            title = entry[2]
            type_ = entry[3]
            year = entry[4]
            parent_title = entry[6]
            parent_type = entry[7]
            parent_year = entry[8]
            grandparent_title = entry[10]
            grandparent_type = entry[11]
            grandparent_year = entry[12]
            actual_title = entry[14]
            
            target_file_path = None  # Ensure this is defined before usage

            if type_ == 'movie':
                base_title = title
                base_year = year
                imdb_id = extract_id(entry[1]) if entry[1] else 'unknown'
                if f"({base_year})" in base_title:
                    folder_name = f"{base_title} {{imdb-{imdb_id}}}"
                else:
                    folder_name = f"{base_title} ({base_year}) {{imdb-{imdb_id}}}"
                target_folder = os.path.join(dest_dir_movies, folder_name)

                if not os.path.exists(target_folder):
                    os.makedirs(target_folder, exist_ok=True)

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
                    target_file_name = f"{base_title}  ({base_year}) {{imdb-{imdb_id}}} [{resolution}]{file_ext}"
                    target_file_name = clean_filename(target_file_name)
                    target_file_path = os.path.join(target_folder, target_file_name)
                    
                    largest_file_path = os.path.join(torrent_dir_path, largest_file)
                    if not os.path.exists(target_file_path):
                        try:
                            relative_source_path = os.path.relpath(largest_file_path, os.path.dirname(target_file_path))
                            os.symlink(relative_source_path, target_file_path)
                        except OSError as e:
                            print(f"Error creating relative symlink: {e}")
                    update_catalog_entry(torrent_dir_name, target_file_path, torrent_dir_name)


            else:
                base_title = grandparent_title if grandparent_title else parent_title if parent_title else title
                base_year = grandparent_year if grandparent_year else parent_year if parent_year else year
                imdb_id = extract_id(entry[9]) if entry[9] else extract_id(entry[5]) if entry[5] else extract_id(entry[1]) if entry[1] else 'unknown'

                if f"({base_year})" in base_title:
                    folder_name = f"{base_title} {{imdb-{imdb_id}}}"
                else:
                    folder_name = f"{base_title} ({base_year}) {{imdb-{imdb_id}}}"
                target_folder = os.path.join(dest_dir, folder_name)

                if not os.path.exists(target_folder):
                    try:
                        os.makedirs(target_folder)
                    except OSError as e:
                        print(f"Error creating target folder: {e}")
                        continue

                for file_name in os.listdir(torrent_dir_path):
                    file_path = os.path.join(torrent_dir_path, file_name)

                    if os.path.isfile(file_path):
                        file_ext = os.path.splitext(file_name)[1]

                        season, episode = extract_season_episode(file_name)
                        if not (season and episode):
                            continue

                        season_folder = f"Season {season}"
                        episode_identifier = f"S{season}E{episode}"

                        target_folder_season = os.path.join(target_folder, season_folder)
                        existing_files = os.listdir(target_folder_season) if os.path.exists(target_folder_season) else []
                        episode_pattern = f"{base_title} ({base_year}) {{imdb-{imdb_id}}} - {episode_identifier} ["
                        if any(f.startswith(episode_pattern) and f.endswith(file_ext) for f in existing_files):
                            update_catalog_entry(torrent_dir_name, None, torrent_dir_name)
                            continue

                        resolution = extract_resolution(file_name, parent_folder_name=torrent_dir_path, file_path=file_path)
                        target_file_name = f"{base_title} ({base_year}) {{imdb-{imdb_id}}} - {episode_identifier} [{resolution}]{file_ext}"
                        target_file_name = clean_filename(target_file_name)

                        if not os.path.exists(target_folder_season):
                            os.makedirs(target_folder_season, exist_ok=True)

                        target_file_path = os.path.join(target_folder_season, target_file_name)

                        if not os.path.exists(file_path):
                            print(f"Source file does not exist: {file_path}")
                        elif not os.path.exists(target_file_path):
                            try:
                                relative_source_path = os.path.relpath(file_path, os.path.dirname(target_file_path))
                                os.symlink(relative_source_path, target_file_path)
                            except OSError as e:
                                print(f"Error creating relative symlink: {e}")
                        update_catalog_entry(torrent_dir_name, target_file_path, torrent_dir_name)


            update_catalog_entry(torrent_dir_name, target_file_path, torrent_dir_name)

            
        except Exception as e:
            print(f"Error processing entry: {e}")

#    write_processed_items_db(new_processed_items)
    
#    processed_items = read_processed_items_db()
#    new_processed_items = set(processed_items)
#    all_dirs = set(os.listdir(src_dir))

#    catalog_dirs = set(entry[13] for entry in catalog_data)
#    catalog_dirs_2 = set(entry[14] for entry in catalog_data)

#    unaccounted_dirs = all_dirs - catalog_dirs - catalog_dirs_2 - processed_items
    
#    if unaccounted_dirs:
#        for unaccounted_dir in unaccounted_dirs:
#            unaccounted_dir_path = os.path.join(src_dir, unaccounted_dir)
#            if os.path.isdir(unaccounted_dir_path):
#                asyncio.run(process_unaccounted_folder(unaccounted_dir_path, dest_dir))
 #               new_processed_items = set(processed_items)
  #              new_processed_items.add(unaccounted_dir)
   #             write_processed_items_db(unaccounted_dir)
    #        else:
     #           print(f"Skipping non-directory: {unaccounted_dir_path}")

def create_symlinks():
    try:
        create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, DATABASE_PATH)
    except Exception as e:
        print(f"Error in create_symlinks: {e}")
    print("create_symlinks function completed.")
