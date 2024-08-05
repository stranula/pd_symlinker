#!/bin/bash

# Check if the /data/torrents directory exists and is not empty
if [ ! -d "/data/torrents" ]; then
  echo "Error: /data/torrents directory does not exist."
  exit 1
elif [ -z "$(ls -A /data/torrents)" ]; then
  echo "Error: /data/torrents directory is empty. Deleting the directory."
  rmdir "/data/torrents"
  exit 1
fi

# Start the main application
exec "$@"
