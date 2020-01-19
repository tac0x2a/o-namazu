#!/usr/bin/env python

import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# from onamazu
from . import config as cfg


class NamazuHandler(PatternMatchingEventHandler):
    def __init__(self, patterns, callback):
        super(NamazuHandler, self).__init__(patterns=patterns)
        self.callback = callback

    def on_moved(self, event):
        print(f"on_moved:{event.src_path} -> {event.dest_path}")

    def on_created(self, event):
        print(f"on_created:{event.src_path}")
        self.callback(event)

    def on_deleted(self, event):
        print(f"on_deleted:{event.src_path}")

    def on_modified(self, event):
        print(f"on_modified:{event.src_path}")
        self.callback(event)


class NamazuWatcher():
    def __init__(self, root_dir_path, config, callback):
        self.root_dir_path = root_dir_path
        self.config = config
        self.callback = callback

    def start(self):
        target_patterns = [v['pattern'] for k, v in self.config.items()]

        print(f"{self.config}")
        print(f'watching {target_patterns} in {self.root_dir_path}')

        event_handler = NamazuHandler(target_patterns, self.callback)
        self.observer = Observer()
        self.observer.schedule(event_handler, self.root_dir_path, recursive=True)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()


# conf = cfg.create_config_map("sample")
# watcher = NamazuWatcher("sample", conf, lambda ev: print(f"callback! : {ev}"))
# try:
#     watcher.start()
#     while True:
#         time.sleep(1)
# except KeyboardInterrupt:
#     watcher.stop()
