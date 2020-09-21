#!/usr/bin/env python

import fnmatch
import time
import datetime
from datetime import timezone, datetime

from pathlib import Path
from threading import Timer

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

# from onamazu
from onamazu import config as cfg
from onamazu import db_file_operator as dfo

import logging
logger = logging.getLogger("o-namazu")


class NamazuEvent():
    def __init__(self, event, config=None):
        self.event = event
        self.src_path = event.src_path
        self.created_at = time.time()
        self.config = config


class NamazuHandler(PatternMatchingEventHandler):
    def __init__(self, patterns, config, callback, confifg_updated_callback):
        patterns = list(set(patterns))
        super(NamazuHandler, self).__init__(patterns=patterns)
        self.config = config
        self.callback = callback
        self.confifg_updated_callback = confifg_updated_callback
        self.last_modified = {}

    def on_moved(self, event):
        logger.debug(f"on_moved:{event.src_path} -> { event.dest_path}")
        self.judge_conf(event)

    def on_created(self, event):
        logger.debug(f"on_created:{event.src_path}")
        self.judge_conf(event)

    def on_deleted(self, event):
        logger.debug(f"on_deleted:{event.src_path}")
        self.judge_conf(event)

    def on_modified(self, event):
        logger.debug(f"on_modified:{event.src_path}")
        self.judge(event)
        self.judge_conf(event)

    def judge_conf(self, event):
        src = event.src_path
        src_path = Path(src)
        event = NamazuEvent(event)

        # Is it configfile
        if src_path.name == cfg.ConfigFileName:
            logger.debug(f"{src_path} is modified.")
            if self.confifg_updated_callback is not None:
                self.confifg_updated_callback(event)
            return

    def judge(self, event):
        src = event.src_path
        src_path = Path(src)
        file_name = src_path.name

        # Is it file?
        if not src_path.is_file():
            logger.debug(f"Ignore '{src}': Is not a file")
            return

        # Is it configfile
        if src_path.name == cfg.ConfigFileName:
            return

        # Is observed directory?
        parent = str(src_path.parent)
        if parent not in self.config:
            logger.debug(f"Ignore '{src}': Not observed directory '{parent}'")
            return

        conf = self.config[parent]
        event = NamazuEvent(event, conf)

        # Is observed file pattern?
        if 'pattern' not in conf:
            logger.warn(f"Ignore '{src}': 'pattern' is not defined in '{parent}'/onamazu.conf")
            return

        pattern = conf['pattern']
        if not fnmatch.fnmatch(file_name, pattern):
            logger.debug(f"Ignore '{src}': Is not matched observed file pattern('{pattern})")
            return

        # Is o-namazu created DB file ?
        db_file = conf['db_file']
        if db_file == file_name:
            logger.debug(f"Ignore '{src}': db_file will be ignored('{db_file})")
            return

        # Is duplicated
        #  modified event?
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
            dfo.update_watching_file(Path(event.src_path), event.config, self._update_last_detected, event)
            self.callback(event)
            # Todo: Need to remove entry last_modified[event.src_path]. But it is required for detecting duplicated events. It looks good the time to remove, when sweeping is implemented.

        else:
            logger.debug(f"Skipped {event}")

    def _update_last_detected(self, file_path, config, file_db, event):
        modTimesinceEpoc = datetime.now().timestamp()
        file_db["last_detected"] = modTimesinceEpoc
        return None


class NamazuWatcher():
    def __init__(self, root_dir_path, config, callback, confifg_updated_callback=None):
        self.root_dir_path = root_dir_path
        self.config = config
        self.callback = callback
        self.confifg_updated_callback = confifg_updated_callback

    def start(self):
        target_patterns = [v['pattern'] for k, v in self.config.items()]
        target_patterns.append('*' + cfg.ConfigFileName)

        logger.debug(f"{self.config}")
        logger.debug(f'watching {target_patterns} in {self.root_dir_path}')

        event_handler = NamazuHandler(target_patterns, self.config, self.callback, self.confifg_updated_callback)
        self.observer = Observer()
        self.observer.schedule(event_handler, self.root_dir_path, recursive=True)
        self.observer.start()

    def wait(self, n):
        self.observer.join(n)

    def stop(self):
        self.observer.stop()
        self.observer.join()
