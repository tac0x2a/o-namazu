from datetime import datetime, timezone, timedelta
import zipfile
import schedule

from pathlib import Path

from onamazu import config
from onamazu import watcher
from onamazu import sweeper

from . import conftest as ct


def test_sweep_directory_list_return_expired_file_list():
    ct.place_config_file("", {"pattern": "*", "ttl": 10})
    conf = config.create_config_map(ct.ROOT_DIR)

    events = []
    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    file_path_old = ct.write_csv("", "test_old.csv", [("hello", "world"), (1, 2), (3, 4)])
    file_path_new = ct.write_csv("", "test_new.csv", [("hello", "world"), (1, 2), (3, 4)])
    w.wait(1)
    w.stop()

    # modify last_detected for debugging
    last_detected = datetime(2020, 7, 19, 0, 0, 0, 0, timezone(timedelta(hours=0)))
    ct.mod_last_detected(file_path_old, last_detected)
    ct.mod_last_detected(file_path_new, last_detected + timedelta(seconds=10))

    db = ct.read_db_file(ct.ROOT_DIR)

    current_time = last_detected + timedelta(seconds=10)
    conf_dir = conf[ct.ROOT_DIR]
    actual_sweep_targets = sweeper._sweep_directory_list_target(Path(ct.ROOT_DIR), db, conf_dir, current_time)
    expected_sweep_targets = [file_path_old]

    assert expected_sweep_targets == actual_sweep_targets


def test_sweep_directory_files_into_archive_dir():
    ct.place_config_file("", {"pattern": "*"})
    conf = config.create_config_map(ct.ROOT_DIR)

    events = []
    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    file_path_old = ct.write_csv("", "test_old.csv", [("hello", "world"), (1, 2), (3, 4)])
    file_path_new = ct.write_csv("", "test_new.csv", [("hello", "world"), (1, 2), (3, 4)])
    w.wait(1)
    w.stop()

    archive_target_files = [Path(file_path_old)]
    dir_db = ct.read_db_file(ct.ROOT_DIR)
    dir_conf = conf[ct.ROOT_DIR]

    sweeper._sweep_directory_files(Path(ct.ROOT_DIR), archive_target_files, dir_db, dir_conf)

    assert not Path(file_path_old).exists()
    assert (Path(ct.ROOT_DIR) / dir_conf["archive"]["name"] / Path(file_path_old).name).exists()
    assert Path(file_path_new).exists()


def test_sweep_directory_files_into_archive_already_exists_save_with_datetime_postfix():
    ct.place_config_file("", {"pattern": "*"})
    conf = config.create_config_map(ct.ROOT_DIR)

    dir_conf = conf[ct.ROOT_DIR]

    events = []
    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    file_path_old = ct.write_csv("", "test.csv", [("hello", "world"), (1, 2), (3, 4)])
    w.wait(1)
    dir_db = ct.read_db_file(ct.ROOT_DIR)
    expected_content_old = file_path_old.read_text()

    sweeper._sweep_directory_files(Path(ct.ROOT_DIR), [Path(file_path_old)], dir_db, dir_conf)

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    file_path_new = ct.write_csv("", "test.csv", [("hello", "world"), (5, 6), (7, 8)])
    w.wait(1)
    dir_db = ct.read_db_file(ct.ROOT_DIR)
    expected_content_new = file_path_new.read_text()

    now = datetime(2019, 8, 15, 1, 39, 0, 0 * 1000, timezone(timedelta(hours=-6)))
    sweeper._sweep_directory_files(Path(ct.ROOT_DIR), [Path(file_path_new)], dir_db, dir_conf, now)

    assert not Path(file_path_old).exists()
    archived_file_old = Path(ct.ROOT_DIR) / dir_conf["archive"]["name"] / Path(file_path_old).name
    assert archived_file_old.exists()
    actual_content_old = archived_file_old.read_text()
    assert expected_content_old == actual_content_old

    assert not Path(file_path_new).exists()
    archived_file_new = Path(ct.ROOT_DIR) / dir_conf["archive"]["name"] / "test_20190815013900.csv"
    assert archived_file_new.exists()
    actual_content_new = archived_file_new.read_text()
    assert expected_content_new == actual_content_new


def test_sweep_directory_files_delete():
    ct.place_config_file("", {"pattern": "*", "archive": {"type": "delete", "name": "dummy"}})
    conf = config.create_config_map(ct.ROOT_DIR)

    events = []
    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    file_path_old = ct.write_csv("", "test_old.csv", [("hello", "world"), (1, 2), (3, 4)])
    file_path_new = ct.write_csv("", "test_new.csv", [("hello", "world"), (1, 2), (3, 4)])
    w.wait(1)
    w.stop()

    archive_target_files = [Path(file_path_old)]
    dir_db = ct.read_db_file(ct.ROOT_DIR)
    dir_conf = conf[ct.ROOT_DIR]

    sweeper._sweep_directory_files(Path(ct.ROOT_DIR), archive_target_files, dir_db, dir_conf)

    assert not Path(file_path_old).exists()
    assert not (Path(ct.ROOT_DIR) / dir_conf["archive"]["name"]).exists()
    assert Path(file_path_new).exists()


def test_sweep_directory_files_into_archive_zip():
    ct.place_config_file("", {"pattern": "*", "archive": {"type": "zip", "name": "_archive.zip"}})
    conf = config.create_config_map(ct.ROOT_DIR)

    events = []
    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    file_path_old = ct.write_csv("", "test_old.csv", [("hello", "world"), (1, 2), (3, 4)])
    file_path_new = ct.write_csv("", "test_new.csv", [("hello", "world"), (1, 2), (3, 4)])
    w.wait(1)
    w.stop()

    archive_target_files = [Path(file_path_old)]
    dir_db = ct.read_db_file(ct.ROOT_DIR)
    dir_conf = conf[ct.ROOT_DIR]

    sweeper._sweep_directory_files(Path(ct.ROOT_DIR), archive_target_files, dir_db, dir_conf)

    assert Path(file_path_new).exists()
    assert not Path(file_path_old).exists()
    zip_file = Path(ct.ROOT_DIR) / dir_conf["archive"]["name"]
    assert zip_file.exists()

    with zipfile.ZipFile(str(zip_file)) as existing_zip:
        assert Path(file_path_old).name in existing_zip.namelist()


def test_sweep_directory_files_into_archive_zip_already_exists_save_with_datetime_postfix():
    ct.place_config_file("", {"pattern": "*", "archive": {"type": "zip", "name": "_archive.zip"}})
    conf = config.create_config_map(ct.ROOT_DIR)

    dir_conf = conf[ct.ROOT_DIR]

    events = []
    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    file_path_old = ct.write_csv("", "test.csv", [("hello", "world"), (1, 2), (3, 4)])
    w.wait(1)
    dir_db = ct.read_db_file(ct.ROOT_DIR)

    sweeper._sweep_directory_files(Path(ct.ROOT_DIR), [Path(file_path_old)], dir_db, dir_conf)

    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    file_path_new = ct.write_csv("", "test.csv", [("hello", "world"), (5, 6), (7, 8)])
    w.wait(1)
    dir_db = ct.read_db_file(ct.ROOT_DIR)

    now = datetime(2019, 8, 15, 1, 39, 0, 0 * 1000, timezone(timedelta(hours=-6)))
    sweeper._sweep_directory_files(Path(ct.ROOT_DIR), [Path(file_path_new)], dir_db, dir_conf, now)

    zip_file_path = Path(ct.ROOT_DIR) / dir_conf["archive"]["name"]
    files_in_zip = zipfile.ZipFile(zip_file_path).namelist()

    assert not Path(file_path_old).exists()
    archived_file_old = Path(ct.ROOT_DIR) / dir_conf["archive"]["name"] / Path(file_path_old).name
    assert archived_file_old.name in files_in_zip

    assert not Path(file_path_new).exists()
    archived_file_new = Path(ct.ROOT_DIR) / dir_conf["archive"]["name"] / "test_20190815013900.csv"
    assert archived_file_new.name in files_in_zip


def test_sweep_directory():
    ct.place_config_file("", {"pattern": "*", "ttl": 10})
    conf = config.create_config_map(ct.ROOT_DIR)

    events = []
    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()
    file_path_old = ct.write_csv("", "test_old.csv", [("hello", "world"), (1, 2), (3, 4)])
    file_path_new = ct.write_csv("", "test_new.csv", [("hello", "world"), (1, 2), (3, 4)])
    w.wait(1)
    w.stop()

    # modify last_detected for debugging
    last_detected = datetime(2020, 7, 19, 0, 0, 0, 0, timezone(timedelta(hours=0)))
    ct.mod_last_detected(file_path_old, last_detected)
    ct.mod_last_detected(file_path_new, last_detected + timedelta(seconds=10))

    current_time = last_detected + timedelta(seconds=10)
    sweeper.sweep(conf, current_time)

    dir_db = ct.read_db_file(ct.ROOT_DIR)
    dir_conf = conf[ct.ROOT_DIR]

    assert not Path(file_path_old).exists()
    assert (Path(ct.ROOT_DIR) / dir_conf["archive"]["name"] / Path(file_path_old).name).exists()
    assert Path(file_path_new).exists()

    dir_db = ct.read_db_file(ct.ROOT_DIR)
    assert Path(file_path_new).name in dir_db["watching"].keys()
    assert Path(file_path_old).name not in dir_db["watching"].keys()


def test_sweep_scheduled_sweep():
    ct.place_config_file("", {"pattern": "*", "ttl": 2})
    conf = config.create_config_map(ct.ROOT_DIR)

    schedule.every(1).seconds.do(lambda: sweeper.sweep(conf))

    events = []
    w = watcher.NamazuWatcher(ct.ROOT_DIR, conf, lambda ev: events.append(ev))
    w.start()

    file_path_old = ct.write_csv("", "test_old.csv", [("hello", "world"), (1, 2), (3, 4)])
    schedule.run_pending()
    w.wait(2)

    file_path_new = ct.write_csv("", "test_new.csv", [("hello", "world"), (1, 2), (3, 4)])
    schedule.run_pending()
    w.wait(1)

    schedule.run_pending()

    w.stop()

    dir_db = ct.read_db_file(ct.ROOT_DIR)
    dir_conf = conf[ct.ROOT_DIR]

    assert not Path(file_path_old).exists()
    assert (Path(ct.ROOT_DIR) / dir_conf["archive"]["name"] / Path(file_path_old).name).exists()
    assert Path(file_path_new).exists()

    dir_db = ct.read_db_file(ct.ROOT_DIR)
    assert Path(file_path_new).name in dir_db["watching"].keys()
    assert Path(file_path_old).name not in dir_db["watching"].keys()

