import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pd_symlinker import create_symlinks

class FolderMonitor:
    def __init__(self, folder_to_monitor):
        self.folder_to_monitor = folder_to_monitor
        self.observer = Observer()

    def run(self):
        event_handler = self.Handler()
        self.observer.schedule(event_handler, self.folder_to_monitor, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event):
            if event.is_directory:
                return None
            elif event.event_type in ('created', 'modified', 'deleted', 'moved'):
                # Run the create_symlinks function when a file event is detected
                print(f"Change detected: {event.event_type} - {event.src_path}")
                create_symlinks()
                print("create_symlinks() function executed.")

if __name__ == '__main__':
    folder_to_monitor = "/data/torrents"
    print("Monitoring Folder: " + folder_to_monitor)
    monitor = FolderMonitor(folder_to_monitor)
    monitor.run()
