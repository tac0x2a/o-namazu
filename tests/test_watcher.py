
from onamazu import config
from onamazu import watcher

from . import conftest as ct


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


def test_return_ignored_duplicated_events():
    ct.place_config_file("", {"min_mod_interval": 10, "pattern": "*.csv"})  # root configuration
    conf = config.create_config_map(ct.ROOT_DIR)
    events = []

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()

    ct.place_file("", f"hoge.csv", "hello,csv")
    ct.place_file("", f"fuga.csv", "hello,csv")
    ct.place_file("", f"hoge.csv", "hello,csv")  # should be ignore
    ct.place_file("", f"fuga.csv", "hello,csv")  # should be ignore
    ct.place_file("", f"hoge.csv", "hello,csv")  # should be ignore
    w.wait(1)

    w.stop()

    expected = [
        f"{ct.ROOT_DIR}/hoge.csv",
        f"{ct.ROOT_DIR}/fuga.csv"
    ]
    actual = [e.src_path for e in events]
    assert expected == actual


def test_return_delayed_events():
    ct.place_config_file("", {"pattern": "*.csv", "callback_delay": 2})  # root configuration
    conf = config.create_config_map(ct.ROOT_DIR)
    events = []

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    ct.place_file("", f"hoge.csv", "hello,csv")

    w.wait(1)
    assert 0 == len(events)

    w.wait(2)
    assert 1 == len(events)

    w.stop()


def test_ignore_duplicated_events_in_callback_delay():
    ct.place_config_file("", {"pattern": "*.csv", "min_mod_interval": 0, "callback_delay": 2})  # root configuration
    conf = config.create_config_map(ct.ROOT_DIR)
    events = []

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()

    ct.place_file("", f"hoge.csv", "hello,csv"); w.wait(1)  # will be ignored
    ct.place_file("", f"hoge.csv", "hello,csv"); w.wait(1)  # will be ignored
    ct.place_file("", f"hoge.csv", "hello,csv"); w.wait(1)  # will be ignored
    ct.place_file("", f"hoge.csv", "hello,csv"); w.wait(1)
    w.wait(3)  # 1event

    assert 1 == len(events)

    w.stop()


def test_ignore_duplicated_events_in_callback_delay_multi_dir():
    ct.place_config_file("mario", {"pattern": "*.csv", "callback_delay": 2})
    ct.place_config_file("luigi", {"pattern": "*.txt", "callback_delay": 1})
    conf = config.create_config_map(ct.ROOT_DIR)
    events = []

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()

    ct.place_file("mario", f"hoge.csv", "hello,world")
    ct.place_file("luigi", f"hoge.txt", "hello,world")
    w.wait(3)  # 1event

    w.stop()

    expected = [
        f"{ct.ROOT_DIR}/luigi/hoge.txt",
        f"{ct.ROOT_DIR}/mario/hoge.csv",
    ]
    actual = [e.src_path for e in events]
    assert expected == actual
