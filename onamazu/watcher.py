import fnmatch
import logging
import time
from datetime import datetime
from pathlib import Path
from threading import Timer
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

# from onamazu
from onamazu import config as cfg
from onamazu import db_file_operator as dfo


class NamazuEvent():
    def __init__(self, src_path: str, config: dict = None):
        self.src_path = src_path
        self.config = config
        self.created_at = time.time()


class NamazuHandler(PatternMatchingEventHandler):
    def __init__(self, patterns, config, callback, confifg_updated_callback, logger=logging.getLogger("o-namazu")):
        patterns = list(set(patterns))
        super(NamazuHandler, self).__init__(patterns=patterns)
        self.config = config
        self.callback = callback
        self.confifg_updated_callback = confifg_updated_callback
        self.logger = logger
        self.last_modified = {}

    def on_moved(self, event):
        self.logger.debug(f"on_moved:{event.src_path} -> {event.dest_path}")

    def on_created(self, event):
        self.logger.debug(f"on_created:{event.src_path}")
        src_path = Path(event.src_path)
        self.judge_conf(src_path)

    def on_deleted(self, event):
        self.logger.debug(f"on_deleted:{event.src_path}")
        src_path = Path(event.src_path)
        self.judge_conf(src_path)

    def on_modified(self, event):
        self.logger.debug(f"on_modified:{event.src_path}")
        src_path = Path(event.src_path)
        self.judge(src_path)
        self.judge_conf(src_path)

    def judge(self, src_path: Path):
        file_name = src_path.name
        src_str = str(src_path)

        # Is it file?
        if not src_path.is_file():
            self.logger.debug(f"Ignore '{src_path}': Is not a file")
            return

        # Is it configfile
        if src_path.name == cfg.ConfigFileName:
            return

        # Is observed directory?
        parent = str(src_path.parent)
        if parent not in self.config:
            self.logger.debug(f"Ignore '{src_str}': Not observed directory '{parent}'")
            return

        conf = self.config[parent]
        event = NamazuEvent(src_str, conf)

        # Is observed file pattern?
        if 'pattern' not in conf:
            self.logger.warn(f"Ignore '{src_str}': 'pattern' is not defined in '{parent}'/onamazu.conf")
            return

        pattern = conf['pattern']
        if not fnmatch.fnmatch(file_name, pattern):
            self.logger.debug(f"Ignore '{src_str}': Is not matched observed file pattern('{pattern})")
            return

        # Is o-namazu created DB file ?
        db_file = conf['db_file']
        if db_file == file_name:
            self.logger.debug(f"Ignore '{src_str}': db_file will be ignored('{db_file})")
            return

        # Is duplicated
        #  modified event?
        if src_str in self.last_modified:
            diff = event.created_at - self.last_modified[src_str].created_at
            min_mod_interval = conf["min_mod_interval"]
            if diff < min_mod_interval:  # replace value by config
                self.logger.debug(f"Ignore '{src_str}': interval({diff}) is short than min_mod_interval({min_mod_interval})")
                return

        self.logger.debug(f"Received new modified event '{src_str}' @ {event.created_at}")
        self.last_modified[src_str] = event
        Timer(conf["callback_delay"], lambda: self.inject_callback(event)).start()

    def inject_callback(self, event):
        if self.last_modified[event.src_path].created_at == event.created_at:
            dfo.update_watching_file(Path(event.src_path), event.config, self._update_last_detected, event)
            self.callback(event)
            # Todo: Need to remove entry last_modified[event.src_path]. But it is required for detecting duplicated events. It looks good the time to remove, when sweeping is implemented.

        else:
            self.logger.debug(f"Skipped {event}")

    def judge_conf(self, src_path: Path):
        event = NamazuEvent(str(src_path))

        # Is it configfile
        if src_path.name == cfg.ConfigFileName:
            self.logger.debug(f"{src_path} is modified.")
            if self.confifg_updated_callback is not None:
                Timer(1, lambda: self.confifg_updated_callback(event)).start()
            return

    def _update_last_detected(self, file_path, config, file_db, event):
        modTimesinceEpoc = datetime.now().timestamp()
        file_db["last_detected"] = modTimesinceEpoc
        return None


class NamazuWatcher():
    def __init__(self, root_dir_path, config, callback, confifg_updated_callback=None, logger=logging.getLogger("o-namazu")):
        self.root_dir_path = root_dir_path
        self.config = config
        self.callback = callback
        self.confifg_updated_callback = confifg_updated_callback
        self.logger = logger

    def __list_all_files_observed(self, config_map):
        files = []
        for path_str, conf in config_map.items():
            pattern = conf['pattern']
            import glob
            files.extend(glob.glob(str(Path(path_str) / pattern)))
        return files

    def __check_already_exist_files(self, event_handler):
        files = self.__list_all_files_observed(self.config)
        self.logger.info(f'Existing files: {files}')
        for file_path in files:
            event_handler.judge(Path(file_path))

    def start(self):
        target_patterns = [v['pattern'] for k, v in self.config.items()]
        target_patterns.append('*' + cfg.ConfigFileName)

        self.logger.debug(f"{self.config}")
        self.logger.debug(f'watching {target_patterns} in {self.root_dir_path}')

        event_handler = NamazuHandler(target_patterns, self.config, self.callback, self.confifg_updated_callback)
        self.observer = Observer()
        self.observer.schedule(event_handler, self.root_dir_path, recursive=True)
        self.observer.start()

        self.__check_already_exist_files(event_handler)

    def wait(self, n):
        self.observer.join(n)

    def stop(self):
        self.observer.stop()
        self.observer.join()
