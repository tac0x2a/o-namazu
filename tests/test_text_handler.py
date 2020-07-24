import textwrap as tw
from pathlib import Path
from onamazu import config
from onamazu import text_handler as th
from . import conftest as ct


def test_text_read_all_return_all_data():
    ct.place_config_file("", {"pattern": "*.jsonl"})
    conf = config.create_config_map(ct.ROOT_DIR)[ct.ROOT_DIR]
    file_path = ct.place_file("", "test.jsonl", tw.dedent(
        """
        {"hello": "world"}
        {"hoge": "fuga"}
        """).lstrip()
    )

    expected = tw.dedent(
        """
        {"hello": "world"}
        {"hoge": "fuga"}
        """).lstrip()

    actual = th.read(Path(file_path), conf)

    assert expected == actual


def test_text_tail_return_appended_data():
    ct.place_config_file("", {"pattern": "*.jsonl"})
    conf = config.create_config_map(ct.ROOT_DIR)[ct.ROOT_DIR]

    file_path = ct.place_file("", "test.jsonl", tw.dedent(
        """
        {"hello": "world"}
        {"hoge": "fuga"}
        """).lstrip()
    )
    expected = tw.dedent(
        """
        {"hello": "world"}
        {"hoge": "fuga"}
        """).lstrip()
    actual = th.read(Path(file_path), conf)
    assert expected == actual


    file_path = ct.place_file("", "test.jsonl", tw.dedent(
        """
        {"hello": "piyo"}
        {"hoge": "pero"}
        """).lstrip()
    )
    expected = tw.dedent(
        """
        {"hello": "piyo"}
        {"hoge": "pero"}
        """).lstrip()
    actual = th.read(Path(file_path), conf)
    assert expected == actual



def test_text_split_return_single_part():
    src_string = tw.dedent(
        # 18 bytes
        # 17 bytes
        """
        {"hello": "piyo"}
        {"hoge": "pero"}
        """).lstrip()

    expected = [
        tw.dedent(  # total 18 + 17 = 35 bytes
            """
            {"hello": "piyo"}
            {"hoge": "pero"}
            """).lstrip(),
    ]

    actual = th.split(src_string, 35)
    assert expected == actual


def test_text_split_return_multi_part():
    src_string = tw.dedent(
        # 18 bytes
        # 17 bytes
        """
        {"hello": "piyo"}
        {"hoge": "pero"}
        """).lstrip()

    expected = [
        tw.dedent(
            """
            {"hello": "piyo"}
            """).lstrip(),
        tw.dedent(
            """
            {"hoge": "pero"}
            """).lstrip(),
    ]

    actual = th.split(src_string, 1)
    assert expected == actual


def test_text_split_return_multi_part_difference_size():
    src_string = tw.dedent(
        # 27 bytes
        # 10 bytes
        # 16 bytes
        """
        {"helloworld": "piyopiyo"}
        {"h": "p"}
        {"hoge": "pero"}
        """).lstrip()

    expected = [
        tw.dedent(  # 27 bytes
            """
            {"helloworld": "piyopiyo"}
            """).lstrip(),
        tw.dedent(  # 26 bytes
            """
            {"h": "p"}
            {"hoge": "pero"}
            """).lstrip(),
    ]

    actual = th.split(src_string, 30)
    assert expected == actual

