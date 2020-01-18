from onamazu import config

config_map = config.create_config_map("./sample")
print(config_map)  # {'sample': {'pattern': '*.json'}, 'sample/hoge': {'pattern': '*.csv', 'piyo': 'piyo'},...
