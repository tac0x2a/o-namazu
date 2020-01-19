#!/usr/bin/env python

import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# from onamazu
import config as cfg


class NamazuHandler(PatternMatchingEventHandler):
    def __init__(self, patterns):
        super(NamazuHandler, self).__init__(patterns=patterns)

    def on_moved(self, event):
        print(f"on_moved:{event.src_path} -> {event.dest_path}")

    def on_created(self, event):
        print(f"on_created:{event.src_path}")

    def on_deleted(self, event):
        print(f"on_deleted:{event.src_path}")

    def on_modified(self, event):
        print(f"on_modified:{event.src_path}")


class NamazuWatcher():
    def __init__(self, root_dir_path):
        self.root_dir_path = root_dir_path
        pass

    def start(self, global_config={}):
        config = cfg.create_config_map(self.root_dir_path)
        target_extensions = [v['pattern'] for k, v in config.items()]

        print(f'watching {target_extensions}')

        event_handler = NamazuHandler(target_extensions)
        self.observer = Observer()
        self.observer.schedule(event_handler, self.root_dir_path, recursive=True)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()


watcher = NamazuWatcher("onamazu")
try:
    watcher.start()
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    watcher.stop()
