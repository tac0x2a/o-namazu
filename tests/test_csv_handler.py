
import textwrap as tw
from pathlib import Path
from onamazu import csv_handler as ch, config
from . import conftest as ct


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

    (actual, pos) = ch._read_tail(Path(file_path), 0, conf)

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
    (actual_head, pos_head) = ch._read_tail(Path(file_path), 0, conf)
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
    (actual_appended, pos_appended) = ch._read_tail(Path(file_path), pos_head, conf)
    assert expected_appended == actual_appended


def test_csv_read_return_appended_data():
    ct.place_config_file("", {"pattern": "*.csv"})
    conf = config.create_config_map(ct.ROOT_DIR)[ct.ROOT_DIR]

    file_path = ct.write_csv("", "test.csv", [("hello", "world"), (1, 2), (3, 4)])
    expected_head = tw.dedent(
        """
        hello,world
        1,2
        3,4
        """).lstrip()
    actual_head = ch.read(Path(file_path), conf)
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
    actual_appended = ch.read(Path(file_path), conf)
    assert expected_appended == actual_appended


def test_csv_read_return_appended_data_each_file():
    ct.place_config_file("", {"pattern": "*.csv"})
    conf = config.create_config_map(ct.ROOT_DIR)[ct.ROOT_DIR]

    file_path_a = ct.write_csv("", "test_a.csv", [("hello", "world"), (1, 2), (3, 4)])
    expected_head_a = tw.dedent(
        """
        hello,world
        1,2
        3,4
        """).lstrip()
    actual_head_a = ch.read(Path(file_path_a), conf)
    assert expected_head_a == actual_head_a

    file_path_b = ct.write_csv("", "test_b.csv", [("hoge", "fuga"), (1, 2), (3, 4), (5, 6)])
    expected_head_b = tw.dedent(
        """
        hoge,fuga
        1,2
        3,4
        5,6
        """).lstrip()
    actual_head_b = ch.read(Path(file_path_b), conf)
    assert expected_head_b == actual_head_b

    # Append new lines
    file_path_a = ct.write_csv("", "test_a.csv", [(5, 6), (7, 8)])
    expected_appended_a = tw.dedent(
        """
        hello,world
        5,6
        7,8
        """).lstrip()
    actual_appended_a = ch.read(Path(file_path_a), conf)
    assert expected_appended_a == actual_appended_a

    file_path_b = ct.write_csv("", "test_b.csv", [(7, 8), ("hoge", "fuga")])
    expected_appended_b = tw.dedent(
        """
        hoge,fuga
        7,8
        hoge,fuga
        """).lstrip()
    actual_appended_b = ch.read(Path(file_path_b), conf)
    assert expected_appended_b == actual_appended_b


def test_csv_read_return_appended_data_as_db_file():
    ct.place_config_file("", {"pattern": "*.csv", "db_file": "o-namazu.csv"})
    conf = config.create_config_map(ct.ROOT_DIR)[ct.ROOT_DIR]

    file_path = ct.write_csv("", "test.csv", [("hello", "world"), (1, 2), (3, 4)])
    expected_head = tw.dedent(
        """
        hello,world
        1,2
        3,4
        """).lstrip()
    actual_head = ch.read(Path(file_path), conf)
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
    actual_appended = ch.read(Path(file_path), conf)
    assert expected_appended == actual_appended
