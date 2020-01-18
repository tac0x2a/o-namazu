

# Config Tree - Structure

from pathlib import Path
import os


def create_config_tree(root_dir_path: str, conf_file: str = ".onamazu"):
    config_tree = {}

    create_config_tree_sub(Path(root_dir_path), conf_file, {}, config_tree)

    return config_tree


def create_config_tree_sub(dir_path: Path, conf_file_name: str, current_config: map, config_tree: map):

    if not Path.is_dir(dir_path):
        return config_tree

    conf_file_path = dir_path / conf_file_name
    if Path.exists(conf_file_path):
        # Todo: actual load config file
        config_tree[str(dir_path)] = {"conf": "config!!"}

    # Recursive load
    for d in [d for d in dir_path.iterdir() if d.is_dir()]:
        create_config_tree_sub(d, conf_file_name, current_config, config_tree)

    return config_tree


config_tree = create_config_tree("sample")
print(config_tree)
