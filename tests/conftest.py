import pytest
from pathlib import Path
import shutil
import yaml
import csv

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


def place_file(dir_path: str, file_name: str, body: str):
    create_dir(dir_path)
    file_path = "/".join([ROOT_DIR, dir_path, file_name])
    with open(file_path, 'w') as db:
        db.write(body)
    print(f"place file:{file_path}")


def write_csv(dir_path: str, file_name: str, rows: list) -> str:
    create_dir(dir_path)
    file_path = "/".join([ROOT_DIR, dir_path, file_name])
    with open(file_path, 'a') as f:
        w = csv.writer(f)
        w.writerows(rows)

    return file_path


def read_db_file(dir_path: str, db_file_name=config.DefaultConfig['db_file']):
    file_path = Path(dir_path) / db_file_name
    with file_path.open() as f:
        return yaml.safe_load(f)
