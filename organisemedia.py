import os
import re
import shutil
import sqlite3
import difflib
import asyncio
import aiohttp
from colorama import init, Fore, Style

init(autoreset=True)

DATABASE_PATH = '/data/media_database.db'
db_lock = threading.Lock()

LOG_LEVELS = {
    "SUCCESS": {"level": 10, "color": Fore.LIGHTGREEN_EX},
    "INFO": {"level": 20, "color": Fore.LIGHTBLUE_EX},
    "ERROR": {"level": 30, "color": Fore.RED},
    "WARN": {"level": 40, "color": Fore.YELLOW},
    "DEBUG": {"level": 50, "color": Fore.LIGHTMAGENTA_EX}
}

print_lock = asyncio.Lock()
input_lock = asyncio.Lock()

def insert_unaccounted_data(src_dir, file_name, matched_imdb_id, year, symlink_top_folder, symlink_filename):
    with db_lock:
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('''
            INSERT INTO unaccounted (src_dir, file_name, matched_imdb_id, year, symlink_top_folder, symlink_filename)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (src_dir, file_name, matched_imdb_id, year, symlink_top_folder, symlink_filename))
        conn.commit()
        conn.close()

def log_message(log_level, message):
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    if log_level in LOG_LEVELS:
        log_info = LOG_LEVELS[log_level]
        formatted_message = f"{Fore.WHITE}{current_time} | {log_info['color']}{log_level} {Fore.WHITE}| {log_info['color']}{message}"
        colored_message = f"{log_info['color']}{formatted_message}{Style.RESET_ALL}"
        print(colored_message)
    else:
        print(f"Unknown log level: {log_level}")

def are_similar(folder_name, show_name, threshold=0.8):
    folder_name = re.sub(r'[^\w\s]', '', folder_name)
    show_name = re.sub(r'[^\w\s]', '', show_name)
    similarity = difflib.SequenceMatcher(None, folder_name, show_name).ratio()
    return similarity >= threshold

async def create_symlinks(src_dir, dest_dir, force=False, split=False):
    existing_symlinks = load_links(links_pkl)
    ignored_files = load_ignored()
    symlink_created = []
    unaccounted = []

    for root, dirs, files in os.walk(src_dir):
        if contains_episode(files):
            log_message('INFO', f"Processing as show folder: {root}")
            for file in files:
                src_file = os.path.join(root, file)
                if src_file in ignored_files:
                    continue

                symlink_exists = any(
                    src_file == existing_src_file
                    for existing_src_file, _ in existing_symlinks
                )
                if symlink_exists:
                    ignored_files.add(src_file)
                    continue

                episode_match = re.search(r'(.*?)(S\d{2}E\d{2,3}(?:\-E\d{2})?|\b\d{1,2}x\d{2}\b|S\d{2}E\d{2}-?(?:E\d{2})|S\d{2,3} ?E\d{2}(?:\+E\d{2})?)', file, re.IGNORECASE)
                if not episode_match:
                    log_message('INFO', f"Ignored non-episode file in show folder: {file}")
                    continue

                parent_folder_name = os.path.basename(root)
                folder_name = re.sub(r'\s*(S\d{2}.*|Season \d+).*|(\d{3,4}p)', '', parent_folder_name).replace('-', ' ').replace('.', ' ')

                if re.match(r'S\d{2} ?E\d{2}', file, re.IGNORECASE):
                    show_name = re.sub(r'\s*(S\d{2}.*|Season \d+).*', '', parent_folder_name).replace('-', ' ').replace('.', ' ').strip()
                else:
                    show_name = episode_match.group(1).replace('.', ' ').strip()

                if are_similar(folder_name.lower(), show_name.lower()):
                    show_name = folder_name

                name, ext = os.path.splitext(file)

                if '.' in name:
                    new_name = re.sub(r'\.', ' ', name)
                else:
                    new_name = name

                season_number = re.search(r'S(\d{2}) ?E\d{2,3}', episode_match.group(2), re.IGNORECASE).group(1)
                season_folder = f"Season {int(season_number):02d}"

                show_folder = re.sub(r'\s+$|_+$|-+$|(\()$', '', show_name).rstrip()

                if show_folder.isdigit() and len(show_folder) <= 4:
                    year = None
                else:
                    year = extract_year_from_folder(parent_folder_name) or extract_year(show_folder)
                    if year:
                        show_folder = re.sub(r'\(\d{4}\)$', '', show_folder).strip()
                        show_folder = re.sub(r'\d{4}$', '', show_folder).strip()
                show_folder, showid, media_dir = await get_series_info(show_folder, year, split, force)
                show_folder = show_folder.replace('/', '')

                resolution = extract_resolution(new_name)
                if not resolution:
                    resolution = extract_resolution(parent_folder_name)
                    if resolution is not None:
                        resolution = f"[{resolution}]"

                file_name = re.search(r'(^.*S\d{2}E\d{2})', new_name)
                if file_name:
                    new_name = file_name.group(0) + ' '
                if re.search(r'\{(tmdb-\d+|imdb-tt\d+)\}', show_folder):
                    year = re.search(r'\((\d{4})\)', show_folder).group(1)
                    new_name = get_episode_details(showid, episode_match.group(2), show_folder, year)
                if resolution:
                    new_name = f"{new_name} [{resolution}] {ext}"
                else:
                    new_name = f"{new_name} {ext}"

                new_name = new_name.replace('/', '')
                dest_path = os.path.join(dest_dir, show_folder, season_folder)

                os.makedirs(dest_path, exist_ok=True)
                dest_file = os.path.join(dest_path, new_name)
                if os.path.islink(dest_file):
                    if os.readlink(dest_file) == src_file:
                        continue
                    else:
                        new_name = get_unique_filename(dest_path, new_name)
                        dest_file = os.path.join(dest_path, new_name)

                if os.path.exists(dest_file) and not os.path.islink(dest_file):
                    ignored_files.add(dest_file)
                    continue

                if os.path.isdir(src_file):
                    shutil.copytree(src_file, dest_file, symlinks=True)
                else:
                    relative_source_path = os.path.relpath(src_file, os.path.dirname(dest_file))
                    os.symlink(relative_source_path, dest_file)
                    existing_symlinks.add((src_file, dest_file))
                    save_link(existing_symlinks, links_pkl)
                    symlink_created.append(dest_file)

                clean_destination = os.path.basename(dest_file)
                log_message("SUCCESS", f"Created symlink: {Fore.LIGHTCYAN_EX}{clean_destination} {Style.RESET_ALL}-> {src_file}")

                # Insert unaccounted data into the database
                insert_unaccounted_data(src_dir, file, showid, year, show_folder, new_name)

        else:
            log_message('INFO', f"Processing as movie folder: {root}")
            for file in files:
                src_file = os.path.join(root, file)
                if src_file in ignored_files:
                    continue

                symlink_exists = any(
                    src_file == existing_src_file
                    for existing_src_file, _ in existing_symlinks
                )
                if symlink_exists:
                    ignored_files.add(src_file)
                    continue

                episode_match = re.search(r'(.*?)(S\d{2}E\d{2,3}(?:\-E\d{2})?|\b\d{1,2}x\d{2}\b|S\d{2}E\d{2}-?(?:E\d{2})|S\d{2,3} ?E\d{2}(?:\+E\d{2})?)', file, re.IGNORECASE)
                if not episode_match:
                    pattern = re.compile(r'(?!.* - \d+\.\d+GB)(.*) - (\d{2,3})(?:v2)?\b(?: (\[?\(?\d{3,4}p\)?\]?))?')
                    alt_pattern = re.compile(r'S(\d{1,2}) - (\d{2})')
                    if re.search(pattern, file) or re.search(alt_pattern, file):
                        show_folder, season_number, new_name, media_dir = await process_anime(file, pattern, alt_pattern, split, force)
                        season_folder = f"Season {int(season_number):02d}"
                        is_anime = True
                    else:
                        continue

                if not is_anime:
                    episode_identifier = episode_match.group(2)

                    multiepisode_match = re.search(r'(S\d{2,3} ?E\d{2,3}E\d{2}|S\d{2,3} ?E\d{2}\+E\d{2}|S\d{2,3} ?E\d{2}\-E\d{2})', episode_identifier, re.IGNORECASE)
                    alt_episode_match = re.search(r'\d{1,2}x\d{2}', episode_identifier)
                    edge_case_episode_match = re.search(r'S\d{3} ?E\d{2}', episode_identifier)

                    if multiepisode_match:
                        episode_identifier = re.sub(
                            r'(S\d{2,3} ?E\d{2}E\d{2}|S\d{2,3} ?E\d{2}\+E\d{2}|S\d{2,3} ?E\d{2}\-E\d{2})',
                            format_multi_match,
                            episode_identifier,
                            flags=re.IGNORECASE
                        )
                    elif alt_episode_match:
                        episode_identifier = re.sub(r'(\d{1,2})x(\d{2})', lambda m: f's{int(m.group(1)):02d}e{m.group(2)}', episode_identifier)
                    elif edge_case_episode_match:
                        episode_identifier = re.sub(r'S(\d{3}) ?E(\d{2})', lambda m: f's{int(m.group(1)):d}e{m.group(2)}', episode_identifier)

                    parent_folder_name = os.path.basename(root)
                    folder_name = re.sub(r'\s*(S\d{2}.*|Season \d+).*|(\d{3,4}p)', '', parent_folder_name).replace('-', ' ').replace('.', ' ')

                    if re.match(r'S\d{2} ?E\d{2}', file, re.IGNORECASE):
                        show_name = re.sub(r'\s*(S\d{2}.*|Season \d+).*', '', parent_folder_name).replace('-', ' ').replace('.', ' ').strip()
                    else:
                        show_name = episode_match.group(1).replace('.', ' ').strip()

                    if are_similar(folder_name.lower(), show_name.lower()):
                        show_name = folder_name

                    name, ext = os.path.splitext(file)

                    if '.' in name:
                        new_name = re.sub(r'\.', ' ', name)
                    else:
                        new_name = name

                    season_number = re.search(r'S(\d{2}) ?E\d{2,3}', episode_identifier, re.IGNORECASE).group(1)
                    season_folder = f"Season {int(season_number):02d}"

                    show_folder = re.sub(r'\s+$|_+$|-+$|(\()$', '', show_name).rstrip()

                    if show_folder.isdigit() and len(show_folder) <= 4:
                        year = None
                    else:
                        year = extract_year_from_folder(parent_folder_name) or extract_year(show_folder)
                        if year:
                            show_folder = re.sub(r'\(\d{4}\)$', '', show_folder).strip()
                            show_folder = re.sub(r'\d{4}$', '', show_folder).strip()
                    show_folder, showid, media_dir = await get_series_info(show_folder, year, split, force)
                    show_folder = show_folder.replace('/', '')

                    resolution = extract_resolution(new_name)
                    if not resolution:
                        resolution = extract_resolution(parent_folder_name)
                        if resolution is not None:
                            resolution = f"[{resolution}]"

                    file_name = re.search(r'(^.*S\d{2}E\d{2})', new_name)
                    if file_name:
                        new_name = file_name.group(0) + ' '
                    if re.search(r'\{(tmdb-\d+|imdb-tt\d+)\}', show_folder):
                        year = re.search(r'\((\d{4})\)', show_folder).group(1)
                        new_name = get_episode_details(showid, episode_identifier, show_folder, year)
                    if resolution:
                        new_name = f"{new_name} [{resolution}] {ext}"
                    else:
                        new_name = f"{new_name} {ext}"

                    new_name = new_name.replace('/', '')
                    dest_path = os.path.join(dest_dir, show_folder, season_folder)

                    os.makedirs(dest_path, exist_ok=True)
                    dest_file = os.path.join(dest_path, new_name)
                    if os.path.islink(dest_file):
                        if os.readlink(dest_file) == src_file:
                            continue
                        else:
                            new_name = get_unique_filename(dest_path, new_name)
                            dest_file = os.path.join(dest_path, new_name)

                    if os.path.exists(dest_file) and not os.path.islink(dest_file):
                        ignored_files.add(dest_file)
                        continue

                    if os.path.isdir(src_file):
                        shutil.copytree(src_file, dest_file, symlinks=True)
                    else:
                        relative_source_path = os.path.relpath(src_file, os.path.dirname(dest_file))
                        os.symlink(relative_source_path, dest_file)
                        existing_symlinks.add((src_file, dest_file))
                        save_link(existing_symlinks, links_pkl)
                        symlink_created.append(dest_file)

                    clean_destination = os.path.basename(dest_file)
                    log_message("SUCCESS", f"Created symlink: {Fore.LIGHTCYAN_EX}{clean_destination} {Style.RESET_ALL}-> {src_file}")

                    # Insert unaccounted data into the database
                    insert_unaccounted_data(src_dir, file, showid, year, show_folder, new_name)

    save_ignored(ignored_files)
    return symlink_created

def contains_episode(files):
    episode_pattern = re.compile(r'(.*?)(S\d{2}E\d{2,3}(?:\-E\d{2})?|\b\d{1,2}x\d{2}\b|S\d{2}E\d{2}-?(?:E\d{2})|S\d{2,3} ?E\d{2}(?:\+E\d{2})?)', re.IGNORECASE)
    for file in files:
        if episode_pattern.search(file):
            return True
    return False

async def get_series_info(series_name, year=None, split=False, force=False):
    global _api_cache
    log_message("INFO", f"Current file: {series_name} year: {year}")
    shows_dir = "shows"
    series_name = series_name.rstrip(string.punctuation)
    formatted_name = series_name.replace(" ", "%20")
    cache_key = f"series_{formatted_name}_{year}"
    if cache_key in _api_cache:
        return _api_cache[cache_key]
    
    search_url = f"https://v3-cinemeta.strem.io/catalog/series/top/search={formatted_name}.json"
    response = requests.get(search_url, timeout=10)
    if response.status_code != 200:
        raise Exception(f"Error searching for series: {response.status_code}")
    
    search_results = response.json()
    metas = search_results.get('metas', [])
    
    selected_index = 0
    if not metas:
        return series_name, None, shows_dir
    
    if force:
        if year:
            for i, meta in enumerate(metas):
                release_info = meta.get('releaseInfo')
                release_info = re.match(r'\b\d{4}\b', release_info).group()
                if release_info and int(year) == int(release_info):
                    selected_index = i
                    break
        selected_meta = metas[selected_index]
        series_id = selected_meta['imdb_id']
        year = selected_meta.get('releaseInfo')
        year = re.match(r'\b\d{4}\b', year).group()
        series_info = f"{selected_meta['name']} ({year}) {{imdb-{series_id}}}"
        if split:
            shows_dir = "anime_shows" if is_anime(get_moviedb_id(series_id)) else "shows"
        _api_cache[cache_key] = (series_info, series_id, shows_dir)
        return series_info, series_id, shows_dir
    
    if not year:
        if len(metas) > 1 and are_similar(metas[0]['name'], metas[1]['name'], 0.9):
            print(Fore.GREEN + f"Found multiple results for '{series_name}, Year: {year}':")
            for i, meta in enumerate(metas[:3]):
                print(Fore.CYAN + f"{i + 1}: {meta['name']} ({meta.get('releaseInfo', 'Unknown year')})")
                
            selected_index = await aioconsole.ainput(Fore.GREEN + "Enter the number of the correct result (or press Enter to choose the first option): " + Style.RESET_ALL)
            if selected_index.strip().isdigit() and 1 <= int(selected_index) <= len(metas):
                selected_index = int(selected_index) - 1
            else:
                selected_index = 0
        elif len(metas) > 1 and not are_similar(series_name.lower(), metas[0]['name'].lower()) :
            print(Fore.GREEN + f"Found similar or no matching results for '{series_name}':")
            for i, meta in enumerate(metas[:3]):
                print(Fore.CYAN + f"{i + 1}: {meta['name']} ({meta.get('releaseInfo', 'Unknown year')})")
                
            selected_index = await aioconsole.ainput(Fore.GREEN + "Enter the number of your choice, or enter IMDb ID directly:  " + Style.RESET_ALL)
            if selected_index.lower().startswith('tt'):
                url = f"https://v3-cinemeta.strem.io/meta/series/{selected_index}.json"
                response = requests.get(url)
                if response.status_code == 200:
                    show_data = response.json()
                    if 'meta' in show_data and show_data['meta']:
                            show_info = show_data['meta']
                            imdb_id = show_info.get('imdb_id')
                            show_title = show_info.get('name')
                            year_info = show_info.get('releaseInfo')
                            year_info = re.match(r'\b\d{4}\b', year_info).group()
                            series_info = f"{show_title} ({year_info}) {{imdb-{imdb_id}}}"
                            if split:
                                log_message('DEBUG', f"dir before: {shows_dir}")
                                shows_dir = "anime_shows" if is_anime(get_moviedb_id(imdb_id)) else "shows"
                            _api_cache[cache_key] = (series_info, imdb_id, shows_dir)
                            return series_info, imdb_id, shows_dir
                    else:
                        print("No show found with the provided IMDb ID")
                        return series_name, None, shows_dir
                else:
                    print("Error fetching show information with IMDb ID")
                    return series_name, None, shows_dir
            elif selected_index.strip().isdigit() and 1 <= int(selected_index) <= len(metas):
                selected_index = int(selected_index) - 1
            else:
                selected_index = 0
        else:
            for i, meta in enumerate(metas):
                if are_similar(series_name.lower().strip(), meta.get('name').lower(), .90):
                    selected_index = i
                    break
    else:
        for i, meta in enumerate(metas):
            release_info = meta.get('releaseInfo')
            release_info = re.match(r'\b\d{4}\b', release_info).group()
            if are_similar(series_name.lower().strip(), meta.get('name').lower(), .90):
                if release_info and int(year) == int(release_info):
                    selected_index = i
                    break
                else:
                    selected_index = 0

    
    selected_meta = metas[selected_index]
    series_id = selected_meta['imdb_id']
    year = selected_meta.get('releaseInfo')
    year = re.match(r'\b\d{4}\b', year).group()
    series_info = f"{selected_meta['name']} ({year}) {{imdb-{series_id}}}"
    if split:
        shows_dir = "anime_shows" if is_anime(get_moviedb_id(series_id)) else "shows"
    _api_cache[cache_key] = (series_info, series_id, shows_dir)
    return series_info, series_id, shows_dir

def extract_year(query):
    match = re.search(r'\((\d{4})\)$', query.strip())
    if match:
        return int(match.group(1))
    match = re.search(r'(\d{4})$', query.strip())
    if match:
        return int(match.group(1))
    return None

# Main script execution
if __name__ == '__main__':
    initialize_database()
