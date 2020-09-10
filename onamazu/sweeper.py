from pathlib import Path
from datetime import datetime, timezone

from onamazu import db_file_operator as dfo

import os
import shutil
import logging
import zipfile

logger = logging.getLogger("o-namazu")


def sweep(config, now: datetime = None):
    if now is None:
        now = datetime.now()
    logger.debug(f"Sweeep started. config:{config}, now:{now}")
    dfo.update_all_db_files(config, _sweep_callback, {"now": now})


def _sweep_callback(dbs: dict, config_all: dict, obj: dict):
    now = obj['now']
    for dir, dir_db in dbs.items():
        dir_config = config_all[dir]
        dir_path = Path(dir)

        expired_file_list = _sweep_directory_list_target(dir_path, dir_db, dir_config, now)
        _sweep_directory_files(dir_path, expired_file_list, dir_db, dir_config, now)


def _sweep_directory_list_target(dir_path: Path, dir_db: dict, dir_config: dict, now: datetime) -> list:
    # Path: last_detected
    last_detected_list = {str(dir_path / file): d["last_detected"] for file, d in dir_db["watching"].items()}

    now_timestamp = now.timestamp()
    ttl = dir_config["ttl"]
    if ttl <= 0:  # 0 is soon, -1 is never archive.
        return []

    for f, l_timestamp in last_detected_list.items():
        diff = now_timestamp - l_timestamp
        logger.debug(f"Sweep test: {dir_path / f}: {l_timestamp}. diff = {diff}, ttl = {ttl}")

    return [Path(f) for f, l_timestamp in last_detected_list.items() if now_timestamp - l_timestamp >= ttl]


def _sweep_directory_files(dir_path: Path, files: list, dir_db: dict, dir_config: dict, now: datetime = None):
    if now is None:
        now = datetime.now()

    ttl = dir_config["ttl"]
    archive = dir_config["archive"]
    archive_type = archive.get("type", "directory")
    archive_name = archive.get("name", "_archive")

    # Type: delete
    if archive_type == "delete":
        for file in files:
            try:
                os.remove(str(file))
                logger.info(f"Deleted file '{file}' because ttl({ttl}) is expired.")
            except Exception:
                logger.exception(f"Delete '{file}' failed.")
            finally:
                del dir_db["watching"][str(file.name)]

        return

    archive_path = dir_path / archive_name

    # Type: zip
    if archive_type == "zip":
        with zipfile.ZipFile(str(archive_path), 'a', compression=zipfile.ZIP_DEFLATED) as zip_file:
            for file in files:
                try:
                    zip_file.write(str(file), arcname=file.name)
                    os.remove(str(file))
                    logger.info(f"Archive file '{file}' into zip `{archive_path}` because ttl({ttl}) is expired.")
                except Exception:
                    logger.exception(f"Delete '{file}' failed.")
                finally:
                    del dir_db["watching"][str(file.name)]
        return

    # Type: directory
    if archive_type == "directory":
        if not archive_path.exists():
            archive_path.mkdir(parents=True)

        for file in files:
            try:
                logger.info(f"Archive file '{file}' into `{archive_path}` because ttl({ttl}) is expired.")
                dst_file_path = archive_path / file.name

                if dst_file_path.exists():
                    dst_file_path = archive_path / __generate_name_with_datetime(file, now)
                    logger.warning(f"Archive file '{file}' is already exists in `{archive_path}`. It will be save as '{dst_file_path}")

                shutil.move(str(file), str(dst_file_path))

            except Exception:
                logger.exception(f"Move '{file}' failed.")
            finally:
                del dir_db["watching"][str(file.name)]
        return


def __generate_name_with_datetime(file_path: Path, now: datetime):
    return file_path.stem + '_' + now.strftime('%Y%m%d%H%M%S') + file_path.suffix
