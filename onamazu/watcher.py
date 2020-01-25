#!/usr/bin/env python

import fnmatch
import time
from pathlib import Path
from threading import Timer

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

# from onamazu
from . import config as cfg

import logging
logger = logging.getLogger("o-namazu")


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
        logger.debug(f"on_moved:{event.src_path} -> { event.dest_path}")

    def on_created(self, event):
        logger.debug(f"on_created:{event.src_path}")

    def on_deleted(self, event):
        logger.debug(f"on_deleted:{event.src_path}")

    def on_modified(self, event):
        logger.debug(f"on_modified:{event.src_path}")
        self.judge(NamazuEvent(event))

    def judge(self, event):
        src = event.src_path
        src_path = Path(src)
        file_name = src_path.name

        # Is it file?
        if not src_path.is_file():
            logger.info(f"Ignore '{src}': Is not a file")
            return

        # Is observed directory?
        parent = str(src_path.parent)
        if parent not in self.config:
            logger.info(f"Ignore '{src}': Not observed directory '{parent}'")
            return

        conf = self.config[parent]

        # Is observed file pattern?
        if 'pattern' not in conf:
            logger.warn(f"Ignore '{src}': 'pattern' is not defined in '{parent}'/.onamazu")
            return

        pattern = conf['pattern']
        if not fnmatch.fnmatch(file_name, pattern):
            logger.debug(f"Ignore '{src}': Is not matched observed file pattern('{pattern})")
            return

        # Is duplicated modified event?
        if src in self.last_modified:
            diff = event.created_at - self.last_modified[src].created_at
            min_mod_interval = conf["min_mod_interval"]
            if diff < min_mod_interval:  # replace value by config
                logger.debug(f"Ignore '{src}': interval({diff}) is short than min_mod_interval({min_mod_interval})")
                return

        if src in self.last_modified:
            logger.debug(f"Received new modified event '{src}' @ {event.created_at}")

        self.last_modified[src] = event
        Timer(conf["callback_delay"], lambda: self.inject_callback(event)).start()

    def inject_callback(self, event):
        if self.last_modified[event.src_path] == event:
            self.callback(event)
            # Todo:  remove event from last_modified
        else:
            logger.debug(f"Skipped {event}")


class NamazuWatcher():
    def __init__(self, root_dir_path, config, callback):
        self.root_dir_path = root_dir_path
        self.config = config
        self.callback = callback

    def start(self):
        target_patterns = [v['pattern'] for k, v in self.config.items()]

        logger.debug(f"{self.config}")
        logger.debug(f'watching {target_patterns} in {self.root_dir_path}')

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
