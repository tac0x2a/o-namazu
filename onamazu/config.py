
from pathlib import Path
import yaml

import logging
logger = logging.getLogger("o-namazu")

ConfigFileName = "onamazu.conf"

DefaultConfig = {
    # Minimum modification interval [sec].
    # Modified events will be ignored if it inside of between previous modified and after "min_mod_interval" seconds.
    # Default value is 1. It means all events will be ignored in term of 1 second since last modified.
    "min_mod_interval": 1,

    # Delay of callback from last modification detect [sec]
    # Often, modification events are received several times in continuous writing the file.
    # The event will be ignored that is received inside of between previous modified and after "callback_delay" seconds.
    # After "callback_delay" seconds from received last modification event, the callback is ececution.
    "callback_delay": 0,

    # File name of status file of the directory.
    # It contains current read position,last time of read, and so on.
    "db_file": "onamazu.db",

    # Time to archive the file [sec]
    # When expired ttl seconds since last detected at by o-namazu, the file will be moved into archive directory.
    # o-namazu will traverse directories each minutes to judge the file should be archived or not.
    # If the value is -1, the file is never archive. (Default)
    "ttl": -1,

    # Destination of ttl expired files [Dict]
    # "type" is "directory", "zip" or "delete"
    # "name" is name of directory or zip as the destination. This is ignored when use "delete" type
    "archive": {
        "type": "directory",
        "name": "_archive"
    }
}

DefaultConfig_MQTT = {
    # MQTT Broker host or IP address.
    "host": "localhost",

    # MQTT Broker port.
    "port": 1883,

    # Topic of published mqtt message.
    "topic": "o-namazu",

    # The file format `csv` or `text`.
    # If use `csv`, when some rows append to the file, o-namazu will send header and appended rows only. When use `text`, just will send appended lines.
    # Default value is `text`.
    "format": "text",

    # Max size of each message is sent. [byte]
    # Default value is 500000 byte (500K).
    "length": 500000
}


def create_config_map(root_dir_path: str, default_conf=DefaultConfig) -> dict:
    """
    Load config parameters each dirs.
    Config file should be written in YAML format.

    Usage:
        config_map = create_config_map("./sample")
        print(config_map) # {'sample': {'pattern': '*.json'}, 'sample/hoge': {'pattern': '*.csv', 'piyo': 'piyo'},...

    Arguments:
        root_dir_path {str} -- Path to root dir.

    Returns:
        dict -- Key: dir path, Value: dict of parameters.
    """

    # Default Values
    config_map = {}

    create_config_map_sub(Path(root_dir_path), ConfigFileName, default_conf, config_map)

    return config_map


def create_config_map_sub(dir_path: Path, conf_file_name: str, current_config: dict, config_map: dict) -> dict:
    if not Path.is_dir(dir_path):
        return config_map

    conf_file_path = dir_path / conf_file_name
    if Path.exists(conf_file_path):
        parsed = parse_config(conf_file_path)
        current_config = dict(current_config, **parsed)  # overwrite parent dir config by current dir config
        config_map[str(dir_path)] = current_config

    # Recursive load
    for d in [d for d in dir_path.iterdir() if d.is_dir()]:
        create_config_map_sub(d, conf_file_name, current_config, config_map)

    return config_map


def parse_config(file_path: Path) -> dict:
    with file_path.open() as f:
        return yaml.safe_load(f)
