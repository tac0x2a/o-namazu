
from onamazu import csv_handler as ch

from onamazu import config
from . import conftest as ct

import textwrap as tw


def test_csv_read_all_return_all_data():
    ct.place_config_file("", {"pattern": "*.csv"})
    conf = config.create_config_map(ct.ROOT_DIR)[ct.ROOT_DIR]
    file_path = ct.create_csv("", "test.csv", ("hello", "world"), [(1, 2), (3, 4)])

    expected = tw.dedent(
        """
        hello,world
        1,2
        3,4
        """).lstrip()

    actual = ch.read_all(file_path, conf)

    assert expected == actual
