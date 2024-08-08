def create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, catalog_path):
    catalog_data = read_catalog_db()
    processed_items = {entry[5] for entry in catalog_data if entry[5]}

    for entry in catalog_data:
        try:
            eid = entry[1]
            torrent_dir_name = entry[13]
            actual_title_name = entry[14]
            original_torrent_dir_name = entry[13]
            original_actual_name = entry[14]

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
                    print(f"Created target folder: {target_folder}")

                torrent_dir_path = find_best_match(torrent_dir_name, actual_title, src_dir)
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
                            update_catalog_entry(torrent_dir_name, target_file_path, original_torrent_dir_name, original_actual_name)
                            print(f"Created relative symlink: {target_file_path} -> {relative_source_path}")
                        except OSError as e:
                            print(f"Error creating relative symlink: {e}")
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
                        
                torrent_dir_path = find_best_match(torrent_dir_name, actual_title, src_dir)
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
                            print(f"Symlink for {episode_identifier} already exists. Skipping file: {file_name}...Is this causing me problems???")
                            update_catalog_entry(torrent_dir_name, target_folder, original_torrent_dir_name, original_actual_name)
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
                                update_catalog_entry(torrent_dir_name, target_file_path, original_torrent_dir_name, original_actual_name)
                                print(f"Created relative symlink: {target_file_path} -> {relative_source_path}")

                            except OSError as e:
                                print(f"Error creating relative symlink: {e}")

            update_catalog_entry(torrent_dir_name, target_file_path, original_torrent_dir_name, original_actual_name)
            
        except Exception as e:
            print(f"Error processing entry: {e}")

def create_symlinks():
    try:
        create_symlinks_from_catalog(src_dir, dest_dir, dest_dir_movies, DATABASE_PATH)
    except Exception as e:
        print(f"Error in create_symlinks: {e}")
    print("create_symlinks function completed.")
