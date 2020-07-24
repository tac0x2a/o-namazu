

from onamazu import db_file_operator as dfo
from pathlib import Path


def read(file_path: Path, conf) -> str:
    """Read csv file

    Args:
        file_path (Path): File to the path
        conf (dict): o-namazu config the directory.

    Raises:
        ValueError: If present dir path or is not exist path.

    Returns:
        str: Text read from the file.
    """

    if Path.is_dir(file_path):
        raise ValueError(f"Unexpected path '{str(file_path)}'. Directory path is not allowed.")

    # Update last read pos
    return dfo.update_watching_file(file_path, conf, _read_with_db)


def _read_with_db(file_path: Path, conf: dict, file_db: dict, obj):
    """Read csv file with db file of current directory.

    Args:
        file_path (Path): File to the path
        conf (dict): o-namazu config the directory.
        file_db (dict): Status of files in current directory.
        obj (dict): Receive from read method via dfo.update_watching_file.

    Returns:
        [type]: [description]
    """

    pos = file_db.get("read_completed_pos", 0)
    (body, new_pos) = _read_tail(file_path, pos, conf)
    file_db["read_completed_pos"] = new_pos

    return body


def _read_tail(file_path: Path, already_read_pos: int, conf: dict) -> (str, int):
    """Read csv file from already_read_pos to end of the file.

    Args:
        file_path (Path): File to the path
        already_read_pos (int): Position of the file that is already read.
        conf (dict): o-namazu config the directory.

    Returns:
        (str, int): (read string, last read position)
    """

    with file_path.open() as f:
        read_string = f.readline()  # header
        current = max(f.tell(), already_read_pos)
        f.seek(current)

        read_string += f.read()

        return (read_string, f.tell())


def split(csv_string: str, length: int) -> []:
    if len(csv_string) <= length:
        return [csv_string]

    lines = csv_string.splitlines()
    if len(lines) <= 2:
        return [csv_string]

    head = lines.pop(0)
    result = []

    tmp_joined = head + '\n' + lines.pop(0) + '\n'
    tmp_length = len(tmp_joined)

    for line in lines:
        ln = line + '\n'

        if tmp_length + len(ln) > length:
            result.append(tmp_joined)
            tmp_joined = head + '\n'
            tmp_length = len(tmp_joined)

        tmp_joined += ln
        tmp_length += len(ln)

    result.append(tmp_joined)

    return result
