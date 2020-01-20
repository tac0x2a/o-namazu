import time
from onamazu import config
from onamazu import watcher

events = []

ROOT_DIR = 'sample'
conf = config.create_config_map(ROOT_DIR)
w = watcher.NamazuWatcher(ROOT_DIR, conf, lambda ev: events.append(ev))

try:
    w.start()
    while w.isAlive():
        w.wait(1)
        print(w.isAlive())
except KeyboardInterrupt:
    w.stop()
