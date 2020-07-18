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


def _load_db(file_path: Path) -> dict:
    with file_path.open() as f:
        return yaml.safe_load(f)


def _store_db(file_path: Path, db: dict):
    with file_path.open("w") as f:
        return yaml.dump(db, f)
