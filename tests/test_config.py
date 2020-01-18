#!/usr/bin/env python

import pytest
from onamazu import config


def test_return_dict():
    expected = {}
    actual = config.create_config_map("not_found_dir")
    assert expected == actual
