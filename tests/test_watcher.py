
import time

from onamazu import config
from onamazu import watcher

from . import conftest as ct

events = []


def test_first():
    ct.place_config_file("", {"pattern": "*.csv"})
    time.sleep(1)

    conf = config.create_config_map(ct.ROOT_DIR)

    events = []

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))

    w.start()
    time.sleep(3)
    ct.place_file("", f"sample.csv", "hello,world1")
    print(events)
    ct.place_file("", f"sample2.csv", "hello,world2")
    time.sleep(1)
    print(events)
    ct.place_file("", f"sample3.csv", "hello,world3")
    w.stop()
    print(events)

    expected = 1
    actual = len(events)
    assert expected == actual
