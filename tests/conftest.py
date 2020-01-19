import pytest
from pathlib import Path
import shutil
import yaml

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
