
from . import conftest as ct
from onamazu import config
from onamazu import watcher


def test_reload_on_modify():
    ct.place_config_file("", {"pattern": "*.jsonl"})
    conf = config.create_config_map(ct.ROOT_DIR)
    events = []

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()

    ct.place_file("", "sample.csv", "hello,world1")
    w.wait(1)
    ct.place_config_file("", {"pattern": "*.csv"})
    w.wait(1)
    ct.place_file("", "sample.csv", "hello,world2")
    w.wait(1)

    w.stop()

    expected = 1
    actual = len(events)
    assert expected == actual
