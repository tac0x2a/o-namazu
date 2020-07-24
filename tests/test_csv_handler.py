
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


def test_csv_split_return_multi_part_csv():
    src_string = tw.dedent(
        # 12 bytes
        #  4 bytes
        #  4 bytes
        #  4 bytes
        """
        hello,world
        1,2
        3,4
        5,6
        """).lstrip()

    expected = [
        tw.dedent(  # total 12 + 4 + 4 = 20 byte
            """
            hello,world
            1,2
            3,4
            """).lstrip(),
        tw.dedent(
            """
            hello,world
            5,6
            """).lstrip(),
    ]

    actual = ch.split(src_string, 20)
    assert expected == actual


def test_csv_split_return_multi_part_difference_size():
    src_string = tw.dedent(
        # 12 bytes
        #  6 bytes
        #  6 bytes
        # 10 bytes
        #  6 bytes
        """
        hello,world
        11,22
        33,44
        5555,6666
        77,77
        """).lstrip()

    expected = [
        tw.dedent(  # total 12 + 6 + 6 = 24 byte
            """
            hello,world
            11,22
            33,44
            """).lstrip(),
        tw.dedent( # total 12 + 10 = 22 byte
            """
            hello,world
            5555,6666
            """).lstrip(),
        tw.dedent( # total 12 + 6 = 18 byte
            """
            hello,world
            77,77
            """).lstrip(),
    ]

    actual = ch.split(src_string, 25)
    assert expected == actual


def test_csv_split_return_single_part_if_single_line():
    src_string = tw.dedent(
        """
        hello,world
        1,2
        3,4
        """).lstrip()

    expected = [
        tw.dedent(
            """
            hello,world
            1,2
            """).lstrip(),
        tw.dedent(
            """
            hello,world
            3,4
            """).lstrip()
    ]

    actual = ch.split(src_string, 1)
    assert expected == actual


def test_csv_split_return_single_part_if_smaller_then_length():
    src_string = tw.dedent(
        """
        hello,world
        1,2
        3,4
        5,6
        7,8
        """).lstrip()

    expected = [
        tw.dedent(  # total 12 + 4 + 4 = 20 byte
            """
            hello,world
            1,2
            3,4
            5,6
            7,8
            """).lstrip()
    ]

    actual = ch.split(src_string, 1024)
    assert expected == actual
