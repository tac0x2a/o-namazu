

from onamazu import db_file_operator as dfo
from pathlib import Path


def read(file_path: Path, conf) -> str:
    if Path.is_dir(file_path):
        raise ValueError(f"Unexpected path '{str(file_path)}'. Directory path is not allowed.")

    # Update last read pos
    return dfo.update_watching_file(file_path, conf, _read_with_db)


def _read_with_db(file_path: Path, conf: dict, file_db: dict, obj):
    pos = file_db.get("read_completed_pos", 0)
    (body, new_pos) = _read_tail(file_path, pos, conf)
    file_db["read_completed_pos"] = new_pos

    return body


def _read_tail(file_path: Path, already_read_pos: int, conf) -> (str, int):

    with file_path.open() as f:
        read_string = f.readline()  # header
        current = max(f.tell(), already_read_pos)
        f.seek(current)

        read_string += f.read()

        return (read_string, f.tell())
