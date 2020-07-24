from onamazu import db_file_operator as dfo
from pathlib import Path


def read(file_path: Path, conf) -> str:
    if Path.is_dir(file_path):
        raise ValueError(f"Unexpected path '{str(file_path)}'. Directory path is not allowed.")

    # Update last read pos
    return dfo.update_watching_file(file_path, conf, _read_with_db)


def _read_with_db(file_path: Path, conf: dict, file_db: dict, obj):
    pos = file_db.get("read_completed_pos", 0)

    with file_path.open() as f:
        f.seek(pos)
        (body, new_pos) = (f.read(), f.tell())

    file_db["read_completed_pos"] = new_pos

    return body


def split(string: str, length: int) -> []:
    if len(string) <= length:
        return [string]

    lines = string.splitlines()
    if len(lines) <= 1:
        return [string]

    result = []

    tmp_joined = lines.pop(0) + '\n'
    tmp_length = len(tmp_joined)

    for line in lines:
        ln = line + '\n'

        if tmp_length + len(ln) > length:
            result.append(tmp_joined)
            tmp_joined = ''
            tmp_length = len(tmp_joined)

        tmp_joined += ln
        tmp_length += len(ln)

    result.append(tmp_joined)

    return result
