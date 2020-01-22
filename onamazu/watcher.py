#!/usr/bin/env python

import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

import fnmatch

# from onamazu
from . import config as cfg


class NamazuEvent():
    def __init__(self, event):
        self.event = event
        self.src_path = event.src_path
        self.created_at = time.time()


class NamazuHandler(PatternMatchingEventHandler):
    def __init__(self, patterns, config, callback):
        patterns = list(set(patterns))
        super(NamazuHandler, self).__init__(patterns=patterns)
        self.config = config
        self.callback = callback
        self.last_modified = {}

    def on_moved(self, event):
        print(f"on_moved:{event.src_path} -> {event.dest_path}")

    def on_created(self, event):
        print(f"on_created:{event.src_path}")
        # self.previouse_created = event

    def on_deleted(self, event):
        print(f"on_deleted:{event.src_path}")

    def on_modified(self, event):
        print(f"on_modified:{event.src_path}")
        self.judge(NamazuEvent(event))

    def judge(self, event):
        src_path = Path(event.src_path)
        file_name = src_path.name
        parent = str(src_path.parent)
        conf = self.config[parent]

        if 'pattern' not in conf:
            return

        # Ignore duplicated modified
        if event.src_path in self.last_modified and \
           time.time() - self.last_modified[event.src_path].created_at < 1:  # replace value by config
            print(f"Ignore {event.src_path}")
            return  # ignore

        pattern = conf['pattern']

        if fnmatch.fnmatch(file_name, pattern):
            self.last_modified[event.src_path] = event
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

        event_handler = NamazuHandler(target_patterns, self.config, self.callback)
        self.observer = Observer()
        self.observer.schedule(event_handler, self.root_dir_path, recursive=True)
        self.observer.start()

    def wait(self, n):
        self.observer.join(n)

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
