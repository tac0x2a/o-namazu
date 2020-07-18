import yaml
from pathlib import Path
from threading import Lock


DB_FILE = '.onamazu.db'
Lock = Lock()


def read(file_path: Path, conf) -> str:

    if Path.is_dir(file_path):
        raise ValueError(f"Unexpected path '{str(file_path)}'. Directory path is not allowed.")

    # Update last read pos

    db_file_path = file_path.parent / DB_FILE
    with Lock:
        if not db_file_path.exists():
            _store_db(db_file_path, {
                "watching": {}
            })

        db = _load_db(db_file_path)
        body = _read_with_db(file_path, db, conf)
        _store_db(db_file_path, db)

        return body


def _read_with_db(file_path: Path, db_dict: dict, conf: dict) -> str:

    watching = db_dict["watching"]
    file_name = file_path.name

    if file_name not in watching.keys():
        watching[file_name] = {
            "read_completed_pos": 0
        }

    pos = watching[file_name]["read_completed_pos"]
    (body, new_pos) = _read_tail(file_path, pos, conf)
    watching[file_name]["read_completed_pos"] = new_pos

    return body


def _read_tail(file_path: Path, already_read_pos: int, conf) -> (str, int):

    with file_path.open() as f:
        read_string = f.readline()  # header
        current = max(f.tell(), already_read_pos)
        f.seek(current)

        read_string += f.read()

        return (read_string, f.tell())


def _load_db(file_path: Path) -> dict:

    with file_path.open() as f:
        return yaml.safe_load(f)


def _store_db(file_path: Path, db: dict):
    with file_path.open("w") as f:
        return yaml.dump(db, f)
