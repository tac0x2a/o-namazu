
from unittest.mock import MagicMock, ANY

from . import conftest as ct
from onamazu.onamazu import ONamazu


def test_reload_on_create():
    ct.place_config_file("", {"pattern": "*.csv"})

    o = ONamazu(ct.ROOT_DIR, 60)
    o.event_handler = MagicMock(name="event_handler")
    o.click()

    ct.place_file("sub", "sample01.csv", "hello,world1")
    o.click()

    ct.place_config_file("sub", {"pattern": "*.csv"})
    o.click(); o.click(); o.click()  # apply

    o.event_handler.assert_not_called()
    ct.place_file("sub", "sample02.csv", "hello,world2")
    o.click()
    o.stop()

    o.event_handler.assert_called_once_with(ANY)
    ev, = o.event_handler.call_args.args
    assert ev.src_path == "/".join([ct.ROOT_DIR, "sub", "sample02.csv"])


def test_reload_on_modified():
    ct.place_config_file("", {"pattern": "*.jsonl"})

    o = ONamazu(ct.ROOT_DIR, 60)
    o.event_handler = MagicMock(name="event_handler")
    o.click()

    ct.place_file("", "sample01.csv", "hello,world1")
    o.click()

    ct.place_config_file("", {"pattern": "*.csv"})
    o.click(); o.click(); o.click()  # apply

    o.event_handler.assert_not_called()
    ct.place_file("", "sample02.csv", "hello,world2")
    o.click()
    o.stop()

    o.event_handler.assert_called_once_with(ANY)
    ev, = o.event_handler.call_args.args
    assert ev.src_path == "/".join([ct.ROOT_DIR, "sample02.csv"])


def test_reload_on_delete():
    ct.place_config_file("", {"pattern": "*.csv"})
    ct.place_config_file("sub", {"pattern": "*.csv"})

    o = ONamazu(ct.ROOT_DIR, 60)
    o.event_handler = MagicMock(name="event_handler")
    o.click()

    ct.place_file("sub", "sample01.csv", "hello,world1")
    o.click()

    ct.delete_config_file("sub")
    o.click(); o.click(); o.click()  # apply

    ct.place_file("sub", "sample02.csv", "hello,world2")
    o.click()
    o.stop()

    o.event_handler.assert_called_once_with(ANY)
    ev, = o.event_handler.call_args.args
    assert ev.src_path == "/".join([ct.ROOT_DIR, "sub", "sample01.csv"])
