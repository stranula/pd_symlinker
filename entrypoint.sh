#!/bin/bash

# Check if the /data/torrents directory exists
if [ ! -d "/data/torrents" ]; then
  echo "Error: /data/torrents directory does not exist."
  exit 1
fi

# Start the main application
exec "$@"
