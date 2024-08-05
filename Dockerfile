# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# # Add Entrypoint
# COPY entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh
# ENTRYPOINT ["/entrypoint.sh"]

# Install watchdog
RUN pip install watchdog

# Copy the current directory contents into the container at /app
COPY . /app

# Command to run the folder monitor script
CMD ["python", "folder_monitor.py"]
