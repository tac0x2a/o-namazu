
from unittest.mock import MagicMock, call, ANY

from pathlib import Path

from . import conftest as ct
from onamazu.onamazu import ONamazu
from onamazu.watcher import NamazuEvent


class TestReload:
    def test_reload_on_create(self):
        ct.place_config_file("", {"pattern": "*.csv"})

        o = ONamazu(ct.ROOT_DIR, 60)
        o.event_handler = MagicMock(name="event_handler")
        o.click()

        file01 = ct.place_file("sub", "sample01.csv", "hello,world1")
        o.click()
        o.event_handler.assert_not_called()

        ct.place_config_file("sub", {"pattern": "*.csv"})
        o.click(); o.click(); o.click()  # apply

        file02 = ct.place_file("sub", "sample02.csv", "hello,world2")
        o.click()
        o.stop()

        actual = [args.args[0].src_path for args in o.event_handler.call_args_list]
        assert [str(file01), str(file02)] == actual

    def test_reload_on_modified(self):
        ct.place_config_file("", {"pattern": "*.jsonl"})

        o = ONamazu(ct.ROOT_DIR, 60)
        o.event_handler = MagicMock(name="event_handler")
        o.click()

        file01 = ct.place_file("", "sample01.csv", "hello,world1")
        o.click()
        o.event_handler.assert_not_called()

        ct.place_config_file("", {"pattern": "*.csv"})
        o.click(); o.click(); o.click()  # apply

        file02 = ct.place_file("", "sample02.csv", "hello,world2")
        o.click()
        o.stop()

        actual = [args.args[0].src_path for args in o.event_handler.call_args_list]
        assert [str(file01), str(file02)] == actual


    def test_reload_on_delete(self):
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
