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
import threading
# from organisemedia import process_unaccounted_folder
import time


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
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('SELECT * FROM catalog')
        rows = c.fetchall()
        conn.close()
        return rows


def update_catalog_entry(processed_dir_name, final_symlink_path, id):
    with db_lock:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''
            UPDATE catalog
            SET processed_dir_name = ?, final_symlink_path = ?
            WHERE id = ?
        ''', (processed_dir_name, final_symlink_path, id))
        conn.commit()
        conn.close()


def extract_year(query):
    match = re.search(r'[(.\s_-](\d{4})[).\s_-]', query.strip())
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


def find_best_match(torrent_file_name, actual_title, src_dir):
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
            (torrent_file_name, dirs),
            (actual_title, dirs),
            (sanitize_title(torrent_file_name), dirs),
            (sanitize_title(actual_title), dirs),
            (sanitize_title(torrent_file_name), sanitized_dirs.keys()),
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
                    best_match, score = process.extractOne(torrent_file_name, [largest_file], scorer=fuzz.ratio)
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


def strip_extension(name):
    return re.sub(r'\.\w{2,4}$', '', name)  # Removes common file extensions (e.g., .mp4, .mkv, .avi)


def create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, catalog_path):
    catalog_data = read_catalog_db()

    for entry in catalog_data:
        if not entry[15]:
            try:
                id = entry[0]
                eid = entry[1]
                title = entry[2]
                type_ = entry[3]
                year = entry[4]
                parent_eid = entry[5]
                parent_title = entry[6]
                parent_type = entry[7]
                parent_year = entry[8]
                grandparent_eid = entry[9]
                grandparent_title = entry[10]
                grandparent_type = entry[11]
                grandparent_year = entry[12]
                torrent_file_name = entry[13]
                catalog_torrent_file_name = entry[13]
                actual_title = entry[14]
                catalog_actual_title = entry[14]
                processed_dir_name = entry[15]
                final_symlink_path = entry[16]

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
                        print(f"Created target folder: {target_folder}")

                    torrent_dir_path = find_best_match(torrent_file_name, actual_title, src_dir)
                    if not torrent_dir_path:
                        continue
                    print(f"Processing torrent directory: {torrent_dir_path}")

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
                                print(f"Created relative symlink: {target_file_path} -> {relative_source_path}")
                            except OSError as e:
                                print(f"Error creating relative symlink: {e}")
                                target_folder = None

                        else:
                            print(f"Symlink already exists: {target_file_path}")

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
                            print(f"Created target folder: {target_folder}")
                        except OSError as e:
                            print(f"Error creating target folder: {e}")
                            continue

                    torrent_dir_path = find_best_match(torrent_file_name, actual_title, src_dir)
                    if not torrent_dir_path:
                        continue
                    print(f"Processing torrent directory: {torrent_dir_path}")

                    for file_name in os.listdir(torrent_dir_path):
                        file_path = os.path.join(torrent_dir_path, file_name)
                        print(f"Processing file: {file_path}")

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
                                print(f"Symlink for {episode_identifier} already exists. Skipping file: {file_name}")
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
                                    print(f"Created relative symlink: {target_file_path} -> {relative_source_path}")

                                except OSError as e:
                                    print(f"Error creating relative symlink: {e}")
                                    target_folder = None
                if target_folder:
                    update_catalog_entry(torrent_dir_path, target_folder, id)

            except Exception as e:
                print(f"Error processing entry: {e}")

    processed_dir_names = {os.path.basename(entry[15]) for entry in catalog_data if entry[15]}
    src_directories = [d for d in os.listdir(src_dir) if os.path.isdir(os.path.join(src_dir, d))]
    unprocessed_directories = set(src_directories) - processed_dir_names
    print(f"Unprocessed {unprocessed_directories}")

    for dir_name in unprocessed_directories:
        dir_path = os.path.join(src_dir, dir_name)
        print(f"Processing unaccounted folder: {dir_path}")
        process_unaccounted_folder(dir_path, DEST_DIR)


def create_symlinks():
    try:
        create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, DATABASE_PATH)
    except Exception as e:
        print(f"Error in create_symlinks: {e}")
    print("create_symlinks function completed.")


def is_tv_show(folder_name):
    tv_show_patterns = [
        r'[Ss](\d{1,2})[Ee](\d{1,2})',  # S01E01 or similar
        r'[Ss]eason\s*\d{1,2}',  # Season 1 or similar
        r'[Ee]pisode\s*\d{1,2}',  # Episode 1 or similar
        r'\d{1,2}[xX]\d{1,2}',  # 1x01 or similar
        r'[Ss]eason\b'  # "Season" without a number
    ]
    for pattern in tv_show_patterns:
        match = re.search(pattern, folder_name, re.IGNORECASE)
        if match:
            print(f"Matched TV show pattern '{pattern}' in folder name '{folder_name}'")
            return True
    return False


def check_files_for_tv_show(folder_path):
    for file_name in os.listdir(folder_path):
        if os.path.isfile(os.path.join(folder_path, file_name)):
            season, episode = extract_season_episode(file_name)
            if season and episode:
                print(f"Detected season/episode pattern in file: {file_name}")
                return True
    return False


def find_largest_file(folder_path):
    largest_file = None
    largest_size = 0
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            file_size = os.path.getsize(file_path)
            if file_size > largest_size:
                largest_size = file_size
                largest_file = file_name
    return largest_file


def extract_year_from_folder_and_file(folder_name, largest_file):
    # Try to extract the year from the folder name
    folder_year = extract_year(folder_name)
    if folder_year:
        print(f"Found year {folder_year} in folder name: {folder_name}")
        return folder_year

    # Try to extract the year from the largest file name
    file_year = extract_year(largest_file)
    if file_year:
        print(f"Found year {file_year} in largest file name: {largest_file}")
        return file_year

    print("No year found in folder or file names.")
    return None


def extract_resolution_from_folder_and_file(folder_name, largest_file):
    common_resolutions = {
        "480p": "480p",
        "720p": "720p",
        "1080p": "1080p",
        "2160p": "2160p",
        "UHD": "2160p",
    }

    # Try to find resolution in folder name
    for key, value in common_resolutions.items():
        if key.lower() in folder_name.lower():
            print(f"Found resolution {value} in folder name: {folder_name}")
            return value

    # Try to find resolution in the largest file name
    for key, value in common_resolutions.items():
        if key.lower() in largest_file.lower():
            print(f"Found resolution {value} in largest file name: {largest_file}")
            return value

    # Fallback: Attempt to extract resolution using the existing method
    resolution = extract_resolution(largest_file, parent_folder_name=folder_name,
                                    file_path=os.path.join(folder_path, largest_file))
    if resolution:
        print(f"Extracted resolution using MoviePy: {resolution}")
        return resolution

    print("No resolution found in folder or file names.")
    return None


def process_unaccounted_folder(folder_path, dest_dir):
    folder_name = os.path.basename(folder_path)

    # Determine if the folder is likely a TV show or a movie
    if is_tv_show(folder_name):
        print(f"{folder_path} appears to be a TV show based on folder name.")
        return "tv_show"

    # If it's a movie, proceed with finding the largest file and extracting the year
    print(f"{folder_path} appears to be a movie.")

    # Find the largest file in the folder
    largest_file = find_largest_file(folder_path)
    if largest_file:
        print(f"Largest file in the folder: {largest_file}")
    else:
        print("No files found in the folder.")
        return "no_files"

    # Extract the year from the folder name or the largest file
    year = extract_year_from_folder_and_file(folder_name, largest_file)

    # Extract the resolution from the folder name or the largest file
    resolution = extract_resolution_from_folder_and_file(folder_name, largest_file)

    # Here you can continue with additional processing for movies, e.g., creating symlinks, etc.
    print(f"Extracted data - Year: {year}, Resolution: {resolution}")
    return "movie"