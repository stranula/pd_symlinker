#!/bin/bash

# Check if the /data/torrents directory exists and is not empty
if [ ! -d "/torrents" ]; then
  echo "Error: /torrents directory does not exist."
  exit 1
elif [ -z "$(ls -A /torrents)" ]; then
  echo "Error: /torrents directory is empty. Deleting the directory."
  rmdir "/torrents"
  exit 1
fi

# Start the main application
exec "$@"
