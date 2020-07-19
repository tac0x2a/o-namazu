import textwrap as tw
from pathlib import Path
from onamazu import csv_handler as ch, config
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

