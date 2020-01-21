
import time

from onamazu import config
from onamazu import watcher

from . import conftest as ct

events = []


def test_return_create_notify():
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


def test_return_specified_pattern():
    ct.place_config_file("c", {"pattern": "*.csv"})
    ct.place_config_file("c/j", {"pattern": "*.json"})

    conf = config.create_config_map(ct.ROOT_DIR)
    events = []

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()

    ct.place_file("c", f"sample.csv", "hello,csv")
    ct.place_file("c", f"sample.json", '{"hello":"json"}')
    ct.place_file("c/j", f"sample.csv", "hello,csv")
    ct.place_file("c/j", f"sample.json", '{"hello":"json"}')
    w.wait(1)

    w.stop()

    expected = [f"{ct.ROOT_DIR}/c/sample.csv", f"{ct.ROOT_DIR}/c/j/sample.json"]
    actual = [e.src_path for e in events]
    assert expected == actual
