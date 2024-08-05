import time
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler
from pd_symlinker import create_symlinks
import os

def print_directory_structure(path, level=0):
    try:
        for entry in os.listdir(path):
            full_path = os.path.join(path, entry)
            print(" " * (level * 2) + "|-- " + entry)
            if os.path.isdir(full_path):
                print_directory_structure(full_path, level + 1)
    except Exception as e:
        print(f"Error reading directory {path}: {e}")

class FolderMonitor:
    def __init__(self, folder_to_monitor):
        self.folder_to_monitor = folder_to_monitor
        self.observer = Observer()

    def run(self):
        event_handler = self.Handler()
        self.observer.schedule(event_handler, self.folder_to_monitor, recursive=True)
        print(f"Starting polling observer for {self.folder_to_monitor}")
        self.observer.start()
        try:
            while True:
                print("Polling observer is running...")
                time.sleep(30)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event):
            if event.is_directory:
                return None
            else:
                print(f"Event detected: {event.event_type} - {event.src_path}")
                create_symlinks()
                print("create_symlinks() function executed.")

if __name__ == '__main__':
    print("Running Startup Scan")
    create_symlinks()
    folder_to_monitor = os.getenv('SRC_DIR', '')
    print("Monitoring Folder: " + folder_to_monitor)

    # Print the entire app directory structure
    print("Printing the entire app directory structure:")
    print_directory_structure("/")

    # Check if the folder exists
    if not os.path.exists(folder_to_monitor):
        print(f"Error: The folder {folder_to_monitor} does not exist.")
        exit(1)

    print(f"Contents of {folder_to_monitor}: {os.listdir(folder_to_monitor)}")

    monitor = FolderMonitor(folder_to_monitor)
    monitor.run()


