#!/usr/bin/env python

from onamazu import config
from onamazu import watcher

config_map = config.create_config_map("")
print(config_map)  # {'sample': {'pattern': '*.json'}, 'sample/hoge': {'pattern': '*.csv', 'piyo': 'piyo'},...
