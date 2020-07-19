import yaml
from pathlib import Path
from threading import Lock


_Lock = Lock()

_DefaultDB = {
    "watching": {}
}

def update_watching_file(file_path: Path, conf, proc, obj={}):
    """
    proc : lambda(file_path: Path, conf: dict, file_db: dict) -> any

    return: returned value from lambda
    """

    db_file = conf["db_file"]
    db_file_path = file_path.parent / db_file

    with _Lock:
        if not db_file_path.exists():
            _store_db(db_file_path, _DefaultDB)

        db = _load_db(db_file_path)
        watching = db["watching"]

        file_db = watching.get(file_path.name, {})
        result = proc(file_path, conf, file_db, obj)
        watching[file_path.name] = file_db

        _store_db(db_file_path, db)

    return result


def update_all_db_files(conf_all: dict, proc, obj={}):
    """
    proc: proc(dbs: {dir_path_str: db Dict})
    """
    with _Lock:
        db_file_paths = [Path(dir) / conf["db_file"] for (dir, conf) in conf_all.items()]
        dbs = {str(db_file_path.parent): _load_db(db_file_path) for db_file_path in db_file_paths}

        proc(dbs, conf_all, obj)

        for dir_str, conf in conf_all.items():
            db_file_path = Path(dir_str) / conf["db_file"]
            db = dbs[str(db_file_path.parent)]
            _store_db(Path(db_file_path), db)


def load_db_in_dir(dir_path: Path, conf):
    db_file = conf["db_file"]
    db_file_path = dir_path / db_file
    return _load_db(db_file_path)


def _load_db(db_file_path: Path) -> dict:
    with db_file_path.open() as f:
        return yaml.safe_load(f)


def _store_db(db_file_path: Path, db: dict):
    with db_file_path.open("w") as f:
        return yaml.dump(db, f)
