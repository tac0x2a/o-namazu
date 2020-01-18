
from pathlib import Path
import yaml
import os


def create_config_tree(root_dir_path: str, conf_file: str = ".onamazu") -> dict:
    """
    Load config parameters each dirs.
    Config file should be written in YAML format.

    Usage:
        config_tree = create_config_tree("./sample")
        print(config_tree) # {'sample': {'pattern': '*.json'}, 'sample/hoge': {'pattern': '*.csv', 'piyo': 'piyo'},...

    Arguments:
        root_dir_path {str} -- Path to root dir.

    Keyword Arguments:
        conf_file {str} -- Name of config file. (default: {".onamazu"})

    Returns:
        dict -- Key: dir path, Value: dict of parameters.
    """

    config_tree = {}

    create_config_tree_sub(Path(root_dir_path), conf_file, {}, config_tree)

    return config_tree


def create_config_tree_sub(dir_path: Path, conf_file_name: str, current_config: dict, config_tree: dict) -> dict:
    if not Path.is_dir(dir_path):
        return config_tree

    conf_file_path = dir_path / conf_file_name
    if Path.exists(conf_file_path):
        try:
            parsed = parse_config(conf_file_path)
            current_config = dict(current_config, **parsed)  # overwrite parent dir config by current dir config
            config_tree[str(dir_path)] = current_config
        except Exception as e:
            print(e)

    # Recursive load
    for d in [d for d in dir_path.iterdir() if d.is_dir()]:
        create_config_tree_sub(d, conf_file_name, current_config, config_tree)

    return config_tree


def parse_config(file_path: Path) -> dict:
    with file_path.open() as f:
        return yaml.safe_load(f)


config_tree = create_config_tree("sample")
print(config_tree)
