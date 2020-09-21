
from unittest.mock import MagicMock, ANY

from . import conftest as ct

from onamazu.onamazu import ONamazu


def test_reload_on_modify():
    ct.place_config_file("", {"pattern": "*.jsonl"})

    o = ONamazu(ct.ROOT_DIR, 60)
    o.sample_handler = MagicMock(name="sample_handler")
    o.click()
    ct.place_file("", "sample01.csv", "hello,world1")
    o.click()
    ct.place_config_file("", {"pattern": "*.csv"})
    o.click()
    o.click()
    o.click()  # apply
    ct.place_file("", "sample02.csv", "hello,world2")
    o.click()
    o.stop()

    o.sample_handler.assert_called_once_with(ANY)
