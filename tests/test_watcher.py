
import time

from onamazu import config
from onamazu import watcher

from . import conftest as ct

events = []


def test_first():
    ct.place_config_file("sub", {"pattern": "*.csv"})
    conf = config.create_config_map(ct.ROOT_DIR)
    events = []

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()

    ct.place_file("sub", f"sample.csv", "hello,world1")
    w.wait(1)

    w.stop()

    expected = 1
    actual = len(events)
    assert expected == actual
