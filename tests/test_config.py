#!/usr/bin/env python

from onamazu import config

from . import conftest as ct


def test_return_empty_dict():
    expected = {}
    actual = config.create_config_map("not_found_dir")
    assert expected == actual


def test_return_simple_dict():
    ct.place_config_file("piyo/01", {"hello": "onamazu"})

    expected = {f'{ct.ROOT_DIR}/piyo/01': {"hello": "onamazu"}}
    actual = config.create_config_map(ct.ROOT_DIR, default_conf={})
    assert expected == actual


def test_return_dict_overwrite_by_child_dir_configuration():
    # ROOT_DIR {"a": "parent_a", "b": "parent_b"}
    # |-child1 {"a": "child1_a"}
    # |-empty/child2 {"b": "child2_b"}
    ct.place_config_file("", {"a": "parent_a", "b": "parent_b"})  # root configuration
    ct.place_config_file("child1", {"a": "child1_a"})
    ct.place_config_file("empty/child2", {"b": "child2_b"})

    expected = {
        f'{ct.ROOT_DIR}': {"a": "parent_a", "b": "parent_b"},
        f'{ct.ROOT_DIR}/child1': {"a": "child1_a", "b": "parent_b"},
        f'{ct.ROOT_DIR}/empty/child2': {"a": "parent_a", "b": "child2_b"},
    }
    actual = config.create_config_map(ct.ROOT_DIR, default_conf={})
    assert expected == actual


def test_return_default_configs():
    ct.place_config_file("", {})  # root configuration
    expected = {
        f'{ct.ROOT_DIR}': {
            "min_mod_interval": 1,
            "callback_delay": 0,
            "db_file": ".onamazu.db"
        },
    }
    actual = config.create_config_map(ct.ROOT_DIR)
    assert expected == actual
