#!/usr/bin/env python

import pytest
import contextlib
import os
from pathlib import Path
import shutil
import yaml

# import functools

from onamazu import config


@pytest.fixture(scope='function', autouse=True)
def scope_function():
    root_dir = 'onamazu_test'
    child_dirs = ['hoge', 'fuga', 'piyo/01']
    for d in child_dirs:
        Path(root_dir + "/" + d).mkdir(parents=True)

    with open(root_dir + "/" + "piyo/01/.onamazu", 'w') as db:
        yaml.dump({"hello": "onamazu"}, db)

    yield
    shutil.rmtree(root_dir)


def test_return_empty_dict():
    expected = {}
    actual = config.create_config_map("not_found_dir")
    assert expected == actual


def test_return_simple_dict():
    expected = {"onamazu_test/piyo/01": {"hello": "onamazu"}}
    actual = config.create_config_map("onamazu_test")
    assert expected == actual
