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


def process_unaccounted_folder(folder_path, dest_dir):
    try:
        folder_name = os.path.basename(folder_path)
        sanitized_name = sanitize_title(folder_name)
        year = extract_year(folder_name)

        # Determine if this folder is a TV show or a movie
        best_match = search_tv_show_with_year_range(sanitized_name, year, id='tmdb', force=False,
                                                    folder_path=folder_path, range_delta=1)

        if best_match:
            # If it's a TV show
            print(f"Matched TV show: {best_match}")
            series_name, tmdb_id, series_year = best_match.split(' {tmdb-')[0], best_match.split(' {tmdb-')[1][
                                                                                :-1], year

            target_folder = os.path.join(dest_dir, f"{series_name} ({series_year}) {{tmdb-{tmdb_id}}}")
            if not os.path.exists(target_folder):
                os.makedirs(target_folder, exist_ok=True)

            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                if os.path.isfile(file_path):
                    season, episode = extract_season_episode(file_name)
                    season_folder = f"Season {season}" if season else "Season Unknown"
                    episode_identifier = f"S{season}E{episode}" if season and episode else "Unknown Episode"

                    target_season_folder = os.path.join(target_folder, season_folder)
                    if not os.path.exists(target_season_folder):
                        os.makedirs(target_season_folder, exist_ok=True)

                    resolution = extract_resolution(file_name)
                    file_ext = os.path.splitext(file_name)[1]
                    target_file_name = f"{series_name} ({series_year}) - {episode_identifier} [{resolution}]{file_ext}"
                    target_file_name = clean_filename(target_file_name)
                    target_file_path = os.path.join(target_season_folder, target_file_name)

                    if not os.path.exists(target_file_path):
                        try:
                            relative_source_path = os.path.relpath(file_path, os.path.dirname(target_file_path))
                            os.symlink(relative_source_path, target_file_path)
                            print(f"Created symlink: {target_file_path} -> {relative_source_path}")
                        except OSError as e:
                            print(f"Error creating symlink: {e}")

                    log_media_item(file_path, target_file_path, tmdb_id)

        else:
            # If it's a movie
            print(f"No TV show match found, trying as a movie: {sanitized_name}")
            movie_data = search_movie(sanitized_name, year)
            if movie_data:
                movie_name = movie_data['title']
                movie_year = movie_data['release_date'].split('-')[0] if movie_data['release_date'] else "Unknown Year"
                tmdb_id = movie_data['id']

                target_folder = os.path.join(dest_dir_movies, f"{movie_name} ({movie_year}) {{tmdb-{tmdb_id}}}")
                if not os.path.exists(target_folder):
                    os.makedirs(target_folder, exist_ok=True)

                for file_name in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, file_name)
                    if os.path.isfile(file_path):
                        resolution = extract_resolution(file_name)
                        file_ext = os.path.splitext(file_name)[1]
                        target_file_name = f"{movie_name} ({movie_year}) [{resolution}]{file_ext}"
                        target_file_name = clean_filename(target_file_name)
                        target_file_path = os.path.join(target_folder, target_file_name)

                        if not os.path.exists(target_file_path):
                            try:
                                relative_source_path = os.path.relpath(file_path, os.path.dirname(target_file_path))
                                os.symlink(relative_source_path, target_file_path)
                                print(f"Created symlink: {target_file_path} -> {relative_source_path}")
                            except OSError as e:
                                print(f"Error creating symlink: {e}")

                        log_media_item(file_path, target_file_path, tmdb_id)

        # Mark folder as processed
        log_processed_folder(folder_name, "Processed")

    except Exception as e:
        print(f"Error processing unaccounted folder: {e}")
        log_processed_folder(folder_name, f"Error: {e}")


def search_tv_show_with_year_range(query, year, id, force, folder_path, range_delta):
    if year is not None:
        for delta in range(-range_delta, range_delta + 1):
            result = search_tv_show(query, year + delta, id=id, force=force, folder_path=folder_path)
            if result:
                return result
    else:
        result = search_tv_show(query, year, id=id, force=force, folder_path=folder_path)
        if result:
            return result
    return None


def log_processed_folder(folder_name, status):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''INSERT OR IGNORE INTO ProcessedFolders (folder_name, status)
                      VALUES (?, ?)''', (folder_name, status))
    cursor.execute('''UPDATE ProcessedFolders SET status = ? WHERE folder_name = ?''', (status, folder_name))
    conn.commit()
    conn.close()


def log_media_item(src_dir, symlink, tmdb_id=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO MediaItems (src_dir, symlink, tmdb_id)
        VALUES (?, ?, ?)
        ON CONFLICT(src_dir) DO UPDATE SET
        symlink=excluded.symlink,
        tmdb_id=excluded.tmdb_id
    ''', (src_dir, symlink, tmdb_id))
    conn.commit()
    conn.close()


def clean_search_query(query):
    year_match = re.search(r'\((\d{4})\)|\b(\d{4})\b', query)
    year = year_match.group(1) or year_match.group(2) if year_match else None
    if year and int(year) < 1900:
        year = None

    if year and int(year) >= 1900:
        query = re.sub(r'\((\d{4})\)|\b(\d{4})\b', '', query)

    # Remove content inside parentheses, curly braces, and square brackets
    query = re.sub(r'\([^)]*\)|\{[^}]*\}|\[[^\]]*\]', '', query)

    # Define patterns to remove everything after and including the pattern
    patterns_to_remove = [
        r'Season \d+',
        r'Seasons \d+',
        r'S\d{2}',
        r'E\d{2}',
        r'\d{3,4}p',
        r'BluRay',
        r'x\d{3,4}',
        r'HEVC',
        r'\d{1,2}bit',
        r'AAC',
        r'\d+x\d+',
        r'Complete',
        r'Extras',
        r' web ',
    ]

    # Remove everything after and including any of the patterns
    for pattern in patterns_to_remove:
        query = re.sub(f"{pattern}.*", '', query, flags=re.IGNORECASE)

    query = re.sub(r'[._-]', ' ', query)
    query = re.sub(r'\s+', ' ', query).strip()

    return query, year


@lru_cache(maxsize=None)
def search_tv_show(query, year=None, id='tmdb', force=False, folder_path=None):
    api_key = get_api_key()
    if not api_key:
        api_key = prompt_for_api_key()

    query, extracted_year = clean_search_query(query)
    if not year and extracted_year:
        year = extracted_year

    def perform_search(year):
        url = "https://api.themoviedb.org/3/search/tv"
        params = {
            'api_key': api_key,
            'query': query
        }
        if year:
            params['first_air_date_year'] = year

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json().get('results', [])
        except requests.exceptions.RequestException as e:
            print(f"Error fetching TMDb data: {e}")
            return []

    results = perform_search(year)
    if not results and year:
        results = perform_search(int(year) + 1)
    if not results and year:
        results = perform_search(int(year) - 1)

    def sanitize_name(name):
        return re.sub(r'[^a-zA-Z0-9\s]', '', name.lower())

    def find_best_match(results, query, year):
        #print("Results:", results)
        combined_query = f"{query} {year}" if year else query
        query_stripped = combined_query.lower()
        #print("Checking tmdb with:", query_stripped)

        exact_matches = [result for result in results if result['name'].lower() == query_stripped]
        if exact_matches:
            best_exact_match = exact_matches[0]
            tmdb_id = best_exact_match['id']
            show_name = best_exact_match['name']
            show_year = best_exact_match['first_air_date'][:4] if best_exact_match['first_air_date'] else "Unknown Year"
            #print("Exact match found:", f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}")
            return f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}"

        fuzzy_matches = [(result, fuzz.ratio(query_stripped, result['name'].lower())) for result in results]
        if fuzzy_matches:
            best_fuzzy_match = max(fuzzy_matches, key=lambda x: x[1])
            if best_fuzzy_match[1] > 85:
                chosen_show = best_fuzzy_match[0]
                show_name = chosen_show.get('name')
                first_air_date = chosen_show.get('first_air_date')
                show_year = first_air_date.split('-')[0] if first_air_date else "Unknown Year"
                tmdb_id = chosen_show.get('id')
                #print("Fuzzy match found:", f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}")
                return f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}"

        # Additional check for ":" in the result name
        if results:
            result = results[0]
            parts = result['name'].split(':')
            if len(parts) > 1:
                left_part = parts[0].strip().lower()
                #print("Left part:", left_part)
                right_part = parts[1].strip().lower()
                #print("Right part:", right_part)

                combined_query = f"{query} {year}" if year else query
                query_stripped = combined_query.lower()
                #print("Query stripped:", query_stripped)

                if query_stripped in left_part or query_stripped in right_part:
                    tmdb_id = result['id']
                    show_name = result['name']
                    show_year = result['first_air_date'][:4] if result['first_air_date'] else "Unknown Year"
                    #print("Colon match found:", f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}")
                    return f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}"

        # Additional check to remove special characters from the result name and retry matching
        sanitized_results = [(result, sanitize_name(result['name'])) for result in results]
        sanitized_query = sanitize_name(query_stripped)

        sanitized_exact_matches = [result for result, sanitized_name in sanitized_results if sanitized_name == sanitized_query]
        if sanitized_exact_matches:
            best_exact_match = sanitized_exact_matches[0]
            tmdb_id = best_exact_match['id']
            show_name = best_exact_match['name']
            show_year = best_exact_match['first_air_date'][:4] if best_exact_match['first_air_date'] else "Unknown Year"
            #print("Sanitized exact match found:", f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}")
            return f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}"

        sanitized_fuzzy_matches = [(result, fuzz.ratio(sanitized_query, sanitized_name)) for result, sanitized_name in sanitized_results]
        if sanitized_fuzzy_matches:
            best_sanitized_fuzzy_match = max(sanitized_fuzzy_matches, key=lambda x: x[1])
            if best_sanitized_fuzzy_match[1] > 85:
                chosen_show = best_sanitized_fuzzy_match[0]
                show_name = chosen_show.get('name')
                first_air_date = chosen_show.get('first_air_date')
                show_year = first_air_date.split('-')[0] if first_air_date else "Unknown Year"
                tmdb_id = chosen_show.get('id')
                #print("Sanitized fuzzy match found:", f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}")
                return f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}"

        return None

    best_match = find_best_match(results, query, year)
    #print("Best match:", best_match)
    if best_match:
        #print("returning match")
        return best_match

    if year:
        for adjusted_year in [int(year) - 1, int(year) + 1]:
            print(f"Performing search for adjusted year: {adjusted_year}")
            results = perform_search(adjusted_year)
            best_match = find_best_match(results, query, adjusted_year)
            print("Adjusted year best match:", best_match)
            if best_match:
                return best_match

    return None


def search_tv_show_by_id(tmdb_id):
    api_key = get_api_key()
    if not api_key:
        api_key = prompt_for_api_key()

    url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
    params = {
        'api_key': api_key
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        show = response.json()
        show_name = show.get('name')
        first_air_date = show.get('first_air_date')
        show_year = first_air_date.split('-')[0] if first_air_date else "Unknown Year"
        proper_name = f"{show_name} ({show_year}) {{tmdb-{tmdb_id}}}"
        return proper_name
    except requests.exceptions.RequestException as e:
        print(f"Error fetching TMDb data for ID {tmdb_id}: {e}")
        return None

@lru_cache(maxsize=None)
def search_movie(query, year=None):
    api_key = get_api_key()
    if not api_key:
        api_key = prompt_for_api_key()

    url = "https://api.themoviedb.org/3/search/movie"
    params = {
        'api_key': api_key,
        'query': query
    }
    if year:
        params['year'] = year

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json().get('results', [])
        if results:
            return results[0]
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching movie data: {e}")
        return None

def tmdb_search(query):
    api_key = get_api_key()
    if not api_key:
        api_key = prompt_for_api_key()

    url = "https://api.themoviedb.org/3/search/tv"
    params = {
        'api_key': api_key,
        'query': query
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json().get('results', [])
        return results
    except requests.exceptions.RequestException as e:
        print(f"Error fetching TMDb search results: {e}")
        return []

def get_overseer_requests():
    overseer_api_address, overseer_api_key = get_overseer_settings()
    if not overseer_api_address or not overseer_api_key:
        print("Overseer API address or key is not set.")
        return []

    url = f"{overseer_api_address}/api/v1/request"
    headers = {
        "X-Api-Key": overseer_api_key,
        "accept": "application/json"
    }

    all_requests = []
    skip = 0
    take = 2000
    while True:
        params = {
            "take": take,
            "skip": skip,
            "sort": "added"
        }
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if not results:
                break
            all_requests.extend(results)
            skip += take
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Overseer data: {e}")
            break

    return all_requests

def fetch_tmdb_series_name(tmdb_id):
    api_key = get_api_key()
    url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        series_name = data.get('name')
        year = data.get('first_air_date', '').split('-')[0] if data.get('first_air_date') else None
        return series_name, year
    except requests.exceptions.RequestException as e:
        print(f"Error fetching TMDb data for ID {tmdb_id}: {e}")
        return None, None

def update_series_names_from_overseer():
    requests_data = get_overseer_requests()
    tmdb_id_count = 0
    missing_tmdb_id_count = 0
    for request in requests_data:
        media = request.get('media', {})
        tmdb_id = media.get('tmdbId')
        if tmdb_id:
            if request.get('type') == 'tv':
                tmdb_id_count += 1
                series_name, year = get_tmdb_series_name(tmdb_id)
                if not series_name:
                    series_name, year = fetch_tmdb_series_name(tmdb_id)
                    if series_name:
                        store_tmdb_series_name(tmdb_id, series_name, year)
        else:
            missing_tmdb_id_count += 1
            print(f"Missing TMDb ID for request: {request}")

    print(f"Total TMDb IDs found: {tmdb_id_count}")
    print(f"Total requests without TMDb ID: {missing_tmdb_id_count}")


def search_series_using_inverted_index(query):
    inverted_index = build_inverted_index()
    results = search_inverted_index(query, inverted_index)
    print(results)
    return results

def clean_filename(filename):
    """Clean up the filename to avoid double dashes and other inconsistencies."""
    filename = re.sub(r' - - ', ' - ', filename)
    filename = re.sub(r' +', ' ', filename).strip()  # Remove extra spaces
    filename = re.sub(r' -$', '', filename)  # Remove trailing dash
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)  # Remove invalid characters
    return filename

def search_tv_show_with_year_range(query, year, id, force, folder_path, range_delta):
    if year is not None:
        for delta in range(-range_delta, range_delta + 1):
            result = search_tv_show(query, year + delta, id=id, force=force, folder_path=folder_path)
            if result:
                return result
    else:
        result = search_tv_show(query, year, id=id, force=force, folder_path=folder_path)
        if result:
            return result
    return None

FFPROBE_PATH = './ffprobe'

def extract_year(query):
    match = re.search(r'[\(\.\s_-](\d{4})[\)\.\s_-]', query.strip())
    if match:
        year = int(match.group(1))
        if 1900 <= year <= datetime.now().year:
            return year
    return None

def extract_resolution(name, parent_folder_name=None, file_path=None):
    if parent_folder_name:
        resolution_match = re.search(r'(\d{3,4}p)', parent_folder_name, re.IGNORECASE)
        if resolution_match:
            return resolution_match.group(1)

    resolution_match = re.search(r'(\d{3,4}p)', name, re.IGNORECASE)
    if resolution_match:
        return resolution_match.group(1)

    if file_path:
        try:
            if not os.path.exists(FFPROBE_PATH):
                raise FileNotFoundError(f"{FFPROBE_PATH} does not exist")

            result = subprocess.run(
                [FFPROBE_PATH, '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=height,width', '-of', 'csv=p=0', file_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            if result.returncode == 0:
                width, height = result.stdout.strip().split(',')
                return f"{width}x{height}"
            else:
                raise RuntimeError(f"ffprobe failed with error: {result.stderr}")
        except Exception as e:
            print(f"Error using ffprobe: {e}")

    return None

def get_resolution_with_ffprobe(file_path):
    try:
        if not os.path.exists(FFPROBE_PATH):
            raise FileNotFoundError(f"{FFPROBE_PATH} does not exist")

        result = subprocess.run(
            [FFPROBE_PATH, "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "json", file_path],
            capture_output=True,
            text=True
        )
        probe_data = json.loads(result.stdout)
        width = probe_data['streams'][0]['width']
        height = probe_data['streams'][0]['height']
        if width in [720, 1080, 2160]:
            return f"{width}p"
        else:
            return f"{width}x{height}"
    except Exception as e:
        print(f"Error getting resolution with ffprobe: {e}")
        return None

def extract_folder_year(folder_name):
    # Match patterns like (2005), .2005., 2005, or surrounded by spaces
    match = re.search(r'[\(\.\s_-](\d{4})[\)\.\s_-]', folder_name)
    if match:
        year = int(match.group(1))
        if 1900 <= year <= datetime.now().year:
            return year
    return None

def sanitize_title(name):
    return re.sub(r'[^a-zA-Z0-9\s.]', ' ', name).strip()  # Preserve periods

def clean_filename(filename):
    """Clean up the filename to avoid double dashes and other inconsistencies."""
    filename = re.sub(r' - - ', ' - ', filename)
    filename = re.sub(r' +', ' ', filename).strip()  # Remove extra spaces
    filename = re.sub(r' -$', '', filename)  # Remove trailing dash
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)  # Remove invalid characters
    return filename


DB_FILE = 'symlinks.db'


def initialize_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS MediaItems (
            id INTEGER PRIMARY KEY,
            src_dir TEXT UNIQUE,
            symlink TEXT,
            tmdb_id TEXT,
            deprecated INTEGER DEFAULT 0
        )
    ''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS ProcessedFolders (
                        id INTEGER PRIMARY KEY,
                        folder_name TEXT UNIQUE,
                        status TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS MultipleMatches (
                        id INTEGER PRIMARY KEY,
                        original_name TEXT,
                        possible_matches TEXT,
                        solution TEXT,
                        folder_paths TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS WrongPattern (
                        id INTEGER PRIMARY KEY,
                        filename TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS TmdbSeriesNames (
                        tmdb_id INTEGER PRIMARY KEY,
                        series_name TEXT,
                        year INTEGER)''')
    conn.commit()
    conn.close()


def log_media_item(src_dir, symlink, tmdb_id=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO MediaItems (src_dir, symlink, tmdb_id)
        VALUES (?, ?, ?)
        ON CONFLICT(src_dir) DO UPDATE SET
        symlink=excluded.symlink,
        tmdb_id=excluded.tmdb_id
    ''', (src_dir, symlink, tmdb_id))
    conn.commit()
    conn.close()


def mark_folder_deprecated(folder_path):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE MediaItems
        SET deprecated = 1
        WHERE src_dir LIKE ?
    ''', (f"{folder_path}%",))
    conn.commit()
    conn.close()


def mark_folder_active(folder_path):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE MediaItems
        SET deprecated = 0
        WHERE src_dir LIKE ?
    ''', (f"{folder_path}%",))
    conn.commit()
    conn.close()


def get_all_source_folders():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT DISTINCT src_dir
        FROM MediaItems
        WHERE deprecated = 0
    ''')
    folders = [os.path.abspath(row[0]) for row in cursor.fetchall()]
    conn.close()
    return folders


def remove_symlink_entry(symlink_path):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM MediaItems
        WHERE symlink = ?
    ''', (symlink_path,))
    conn.commit()
    conn.close()


def log_multiple_match(original_name, possible_matches, folder_path):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    possible_matches_json = json.dumps(possible_matches)
    folder_paths_json = json.dumps([folder_path])
    cursor.execute('''INSERT INTO MultipleMatches (original_name, possible_matches, folder_paths)
                      VALUES (?, ?, ?)''', (original_name, possible_matches_json, folder_paths_json))
    conn.commit()
    conn.close()


def log_processed_folder(folder_name, status):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''INSERT OR IGNORE INTO ProcessedFolders (folder_name, status)
                      VALUES (?, ?)''', (folder_name, status))
    cursor.execute('''UPDATE ProcessedFolders SET status = ? WHERE folder_name = ?''', (status, folder_name))
    conn.commit()
    conn.close()


def get_processed_folders():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''SELECT folder_name FROM ProcessedFolders''')
    processed_folders = [row[0] for row in cursor.fetchall()]
    conn.close()
    return processed_folders


def get_multiple_matches():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''SELECT original_name, solution FROM MultipleMatches WHERE solution IS NOT NULL''')
    multiple_matches = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    return multiple_matches


def get_unresolved_multiple_matches():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''SELECT id, original_name, possible_matches, folder_paths 
                      FROM MultipleMatches WHERE solution IS NULL''')
    unresolved_matches = cursor.fetchall()
    conn.close()
    matches = []
    for row in unresolved_matches:
        id, original_name, possible_matches, folder_paths = row
        try:
            possible_matches = json.loads(possible_matches)
        except json.JSONDecodeError:
            possible_matches = []
        try:
            folder_paths = json.loads(folder_paths) if folder_paths else []
        except json.JSONDecodeError:
            folder_paths = []
        matches.append((id, original_name, possible_matches, folder_paths))
    return matches


def update_multiple_match_solution(id, solution):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''UPDATE MultipleMatches SET solution = ? WHERE id = ?''', (solution, id))
    conn.commit()
    conn.close()


def delete_multiple_match(id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''DELETE FROM MultipleMatches WHERE id = ?''', (id,))
    conn.commit()
    conn.close()


def log_wrong_pattern(filename):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO WrongPattern (filename)
                      VALUES (?)''', (filename,))
    conn.commit()
    conn.close()


def store_tmdb_series_name(tmdb_id, series_name, year):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO TmdbSeriesNames (tmdb_id, series_name, year)
        VALUES (?, ?, ?)
        ON CONFLICT(tmdb_id) DO UPDATE SET
        series_name=excluded.series_name,
        year=excluded.year
    ''', (tmdb_id, series_name, year))
    conn.commit()
    conn.close()


def get_tmdb_series_name(tmdb_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''SELECT series_name, year FROM TmdbSeriesNames WHERE tmdb_id = ?''', (tmdb_id,))
    result = cursor.fetchone()
    conn.close()
    return result if result else (None, None)


def build_inverted_index():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''SELECT tmdb_id, series_name, year FROM TmdbSeriesNames''')
    series = cursor.fetchall()
    conn.close()

    inverted_index = defaultdict(list)

    for tmdb_id, series_name, year in series:
        ngrams = generate_ngrams(series_name)
        for ngram in ngrams:
            inverted_index[ngram].append((series_name, tmdb_id, year))

    return inverted_index


def generate_ngrams(text, n=3):
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s.]', '', text)  # Keep periods
    words = text.split()
    ngrams = set()
    for word in words:
        for i in range(len(word) - n + 1):
            ngrams.add(word[i:i + n])
    return ngrams


def normalize_text(text):
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s.]', '', text)
    text = text.replace('&', 'and')
    return text


def search_inverted_index(query, inverted_index, year=None, min_matches=2, fuzzy_threshold=80):
    query = normalize_text(query)
    query_ngrams = generate_ngrams(query)
    results = defaultdict(int)

    for ngram in query_ngrams:
        if ngram in inverted_index:
            for entry in inverted_index[ngram]:
                if len(entry) == 3:
                    series_name, tmdb_id, series_year = entry
                    if year is None or series_year == year:
                        results[(series_name, tmdb_id, series_year)] += 1

    filtered_results = {k: v for k, v in results.items() if v >= min_matches}
    sorted_results = sorted(filtered_results.items(), key=lambda item: item[1], reverse=True)
    # print("Sorted Results: " + str(sorted_results))

    combined_query = f"{query} {year}" if year else query
    # print("Combined_query: " + str(combined_query))

    # Apply fuzzy matching to refine results
    fuzzy_results = []
    for entry, match_count in sorted_results:
        series_name, tmdb_id, series_year = entry
        combined_series_name = normalize_text(f"{series_name} {series_year}" if year else f"{series_name}")
        score = fuzz.ratio(combined_query.lower(), combined_series_name.lower())
        if score > fuzzy_threshold:
            fuzzy_results.append(((series_name, tmdb_id, series_year), score))

    sorted_fuzzy_results = sorted(fuzzy_results, key=lambda x: x[1], reverse=True)
    # print("Sorted Fuzzy Results: " + str(sorted_fuzzy_results))

    if not sorted_fuzzy_results:
        fuzzy_results_inverted = []
        for entries in inverted_index.values():
            for entry in entries:
                if len(entry) == 3:
                    series_name, tmdb_id, series_year = entry
                    combined_series_name = normalize_text(f"{series_name} {series_year}" if year else f"{series_name}")
                    score = fuzz.ratio(combined_query.lower(), combined_series_name.lower())
                    if score > fuzzy_threshold:
                        fuzzy_results_inverted.append(((series_name, tmdb_id, series_year), score))
        sorted_fuzzy_results = sorted(fuzzy_results_inverted, key=lambda x: x[1], reverse=True)
        # print("Sorted Fuzzy Results Inverted: " + str(sorted_fuzzy_results))

    # If no good fuzzy results or the best fuzzy result is off by more than 5 years, adjust the query year by ±1 year
    if not sorted_fuzzy_results and year or (year and abs(sorted_fuzzy_results[0][0][2] - year) > 5):
        print("Year mismatch")
        for adjusted_year in [year - 1, year + 1]:
            if adjusted_year < 1900 or adjusted_year > 2100:
                continue
            combined_query = f"{query} {adjusted_year}"
            fuzzy_results = []
            for entry, match_count in sorted_results:
                series_name, tmdb_id, series_year = entry
                combined_series_name = f"{series_name} {series_year}"
                score = fuzz.ratio(combined_query.lower(), combined_series_name.lower())
                if score > fuzzy_threshold:
                    fuzzy_results.append(((series_name, tmdb_id, series_year), score))

            adjusted_sorted_fuzzy_results = sorted(fuzzy_results, key=lambda x: x[1], reverse=True)
            # print(f"Adjusted Year {adjusted_year} - Sorted Fuzzy Results: " + str(adjusted_sorted_fuzzy_results))

            if adjusted_sorted_fuzzy_results and abs(adjusted_sorted_fuzzy_results[0][0][2] - year) <= 5:
                sorted_fuzzy_results = adjusted_sorted_fuzzy_results
                break
            else:
                sorted_fuzzy_results = []
    # print("Returned Fuzzy Results: " + str(sorted_fuzzy_results))
    return sorted_fuzzy_results


# Example usage
def search_series(query):
    inverted_index = build_inverted_index()
    results = search_inverted_index(query, inverted_index)
    for (series_name, tmdb_id, year), score in results:
        print(f"Series: {series_name}, TMDb ID: {tmdb_id}, Year: {year}, Score: {score}")

