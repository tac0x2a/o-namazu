
from onamazu import csv_handler as ch

from onamazu import config
from . import conftest as ct

import textwrap as tw


def test_csv_read_all_return_all_data():
    ct.place_config_file("", {"pattern": "*.csv"})
    conf = config.create_config_map(ct.ROOT_DIR)[ct.ROOT_DIR]
    file_path = ct.write_csv("", "test.csv", [("hello", "world"), (1, 2), (3, 4)])

    expected = tw.dedent(
        """
        hello,world
        1,2
        3,4
        """).lstrip()

    actual = ch.read_all(file_path, conf)

    assert expected == actual


def test_csv_tail_return_appended_data():
    ct.place_config_file("", {"pattern": "*.csv"})
    conf = config.create_config_map(ct.ROOT_DIR)[ct.ROOT_DIR]

    file_path = ct.write_csv("", "test.csv", [("hello", "world"), (1, 2), (3, 4)])
    expected_head = tw.dedent(
        """
        hello,world
        1,2
        3,4
        """).lstrip()
    (actual_head, pos_head) = ch.tail(file_path, 0, conf)
    assert expected_head == actual_head

    # Append new lines
    file_path = ct.write_csv("", "test.csv", [(5, 6), (7, 8), (9, 10)])
    expected_appended = tw.dedent(
        """
        hello,world
        5,6
        7,8
        9,10
        """).lstrip()
    (actual_appended, pos_appended) = ch.tail(file_path, pos_head, conf)

    assert expected_appended == actual_appended

