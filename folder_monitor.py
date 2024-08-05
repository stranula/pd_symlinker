# import time
# from watchdog.observers.polling import PollingObserver as Observer
# from watchdog.events import FileSystemEventHandler
# from pd_symlinker import create_symlinks
# import os

# class FolderMonitor:
#     def __init__(self, folder_to_monitor):
#         self.folder_to_monitor = folder_to_monitor
#         self.observer = Observer()

#     def run(self):
#         event_handler = self.Handler()
#         self.observer.schedule(event_handler, self.folder_to_monitor, recursive=True)
#         print(f"Starting polling observer for {self.folder_to_monitor}")
#         self.observer.start()
#         try:
#             while True:
#                 print("Polling observer is running...")
#                 time.sleep(30)
#         except KeyboardInterrupt:
#             self.observer.stop()
#         self.observer.join()

#     class Handler(FileSystemEventHandler):
#         def on_any_event(self, event):
#             if event.is_directory:
#                 return None
#             else:
#                 print(f"Event detected: {event.event_type} - {event.src_path}")
#                 create_symlinks()
#                 print("create_symlinks() function executed.")

# if __name__ == '__main__':
#     print("Running Startup Scan")
#     create_symlinks()
#     folder_to_monitor = "/data/torrents"
#     print("Monitoring Folder: " + folder_to_monitor)
#     monitor = FolderMonitor(folder_to_monitor)
#     monitor.run()

import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pd_symlinker import create_symlinks
import os

class FolderMonitor:
    def __init__(self, folder_to_monitor):
        self.folder_to_monitor = folder_to_monitor
        self.observer = Observer()

    def run(self):
        event_handler = self.Handler()
        self.observer.schedule(event_handler, self.folder_to_monitor, recursive=True)
        print(f"Starting observer for {self.folder_to_monitor}")
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
            else:
                print(f"Event detected: {event.event_type} - {event.src_path}")
                create_symlinks()
                print("create_symlinks() function executed.")

if __name__ == '__main__':
    print("Running Startup Scan")
    create_symlinks()
    folder_to_monitor = os.getenv('SRC_DIR')
    print("Monitoring Folder: " + folder_to_monitor)
    monitor = FolderMonitor(folder_to_monitor)
    monitor.run()

