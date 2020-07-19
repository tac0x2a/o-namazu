import pytest
from pathlib import Path
import shutil
import yaml
import csv
import datetime

from io import StringIO

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


def place_file(dir_path: str, file_name: str, body: str, last_detected: datetime = None) -> str:
    create_dir(dir_path)
    file_path = Path(ROOT_DIR) / dir_path / file_name
    with file_path.open('a') as db:
        db.write(body)
    print(f"place file:{file_path}")

    if last_detected is None:
       return file_path


def mod_last_detected(file_path: Path, last_detected: datetime):
    dir = file_path.parent

    db = read_db_file(str(dir))
    file_name = file_path.name
    file_db = db["watching"][file_name]
    file_db["last_detected"] = last_detected.timestamp()
    write_db_file(str(dir), db)


def write_csv(dir_path: str, file_name: str, rows: list, last_detected: datetime = None) -> str:
    si = StringIO()
    w = csv.writer(si)
    w.writerows(rows)
    return place_file(dir_path, file_name, si.getvalue(), last_detected)


def read_db_file(dir_path: str, db_file_name=config.DefaultConfig['db_file']):
    file_path = Path(dir_path) / db_file_name
    with file_path.open() as f:
        return yaml.safe_load(f)


def write_db_file(dir_path: str, db: dict, db_file_name=config.DefaultConfig['db_file']):
    file_path = Path(dir_path) / db_file_name
    with file_path.open("w") as f:
        return yaml.dump(db, f)
