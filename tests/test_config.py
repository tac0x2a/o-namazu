#!/usr/bin/env python

import contextlib
import os
import shutil
from pathlib import Path

import pytest
import yaml

from onamazu import config

ROOT_DIR = 'onamazu_test'
@pytest.fixture(scope='function', autouse=True)
def scope_function():
    create_dir("")
    yield
    shutil.rmtree(ROOT_DIR)


def create_dir(dir_path):
    path = Path(ROOT_DIR + "/" + dir_path)
    if not path.exists():
        path.mkdir(parents=True)


def place_config_file(dir_path: str, yaml_body: dict, conf_file_name='.onamazu'):
    create_dir(dir_path)
    conf_file_path = "/".join([ROOT_DIR, dir_path, conf_file_name])
    with open(conf_file_path, 'w') as db:
        yaml.dump(yaml_body, db)


def test_return_empty_dict():
    expected = {}
    actual = config.create_config_map("not_found_dir")
    assert expected == actual


def test_return_simple_dict():
    place_config_file("piyo/01", {"hello": "onamazu"})

    expected = {f'{ROOT_DIR}/piyo/01': {"hello": "onamazu"}}
    actual = config.create_config_map(ROOT_DIR)
    assert expected == actual
