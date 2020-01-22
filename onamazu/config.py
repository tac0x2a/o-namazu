
from pathlib import Path
import yaml

DefaultConfig = {
    # Minimum modification interval [sec].
    # Modified events will be ignored if it inside of between previous modified and after "min_mod_interval" seconds.
    # Default value is 1. It means all events will be ignored in term of 1 second since last modified.
    "min_mod_interval": 1
}


def create_config_map(root_dir_path: str, default_conf=DefaultConfig, conf_file: str = ".onamazu") -> dict:
    """
    Load config parameters each dirs.
    Config file should be written in YAML format.

    Usage:
        config_map = create_config_map("./sample")
        print(config_map) # {'sample': {'pattern': '*.json'}, 'sample/hoge': {'pattern': '*.csv', 'piyo': 'piyo'},...

    Arguments:
        root_dir_path {str} -- Path to root dir.

    Keyword Arguments:
        conf_file {str} -- Name of config file. (default: {".onamazu"})

    Returns:
        dict -- Key: dir path, Value: dict of parameters.
    """

    # Default Values
    config_map = {}

    create_config_map_sub(Path(root_dir_path), conf_file, default_conf, config_map)

    return config_map


def create_config_map_sub(dir_path: Path, conf_file_name: str, current_config: dict, config_map: dict) -> dict:
    if not Path.is_dir(dir_path):
        return config_map

    conf_file_path = dir_path / conf_file_name
    if Path.exists(conf_file_path):
        try:
            parsed = parse_config(conf_file_path)
            current_config = dict(current_config, **parsed)  # overwrite parent dir config by current dir config
            config_map[str(dir_path)] = current_config
        except Exception as e:
            print(e)

    # Recursive load
    for d in [d for d in dir_path.iterdir() if d.is_dir()]:
        create_config_map_sub(d, conf_file_name, current_config, config_map)

    return config_map


def parse_config(file_path: Path) -> dict:
    with file_path.open() as f:
        return yaml.safe_load(f)
