#!/usr/bin/env python

import argparse
import logging
import logging.handlers
import os
import sys
from pathlib import Path
from operator import add
from functools import reduce

import schedule
from onamazu import (config, csv_handler, mqtt_sender, sweeper, text_handler, watcher)

# -------------------------------------------------

# -------------------------------------------------
# Argument Parsing
parser = argparse.ArgumentParser(description='Observe file modification recursively, and callback')
parser.add_argument('observe_directory', help='Target to observe directory')  # Required
parser.add_argument('--archive_interval', help='Traverse interval seconds to judge the file should be archive [sec]', default=60)

parser.add_argument('--log-level', help='Log level', choices=['DEBUG', 'INFO', 'WARN', 'ERROR'], default='INFO')
parser.add_argument('--log-format', help='Log format by \'logging\' package', default='[%(levelname)s] %(asctime)s | %(pathname)s(L%(lineno)s) | %(message)s')  # Optional
parser.add_argument('--log-file', help='Log file path')
parser.add_argument('--log-file-count', help='Log file keep count', type=int, default=1000)
parser.add_argument('--log-file-size', help='Size of each log file', type=int, default=1000000)  # default 1MB
args = parser.parse_args()

Directory = args.observe_directory

# Logger initialize
logging.basicConfig(level=args.log_level, format=args.log_format)

if args.log_file:
    dir = os.path.dirname(args.log_file)
    if not os.path.exists(dir):
        os.makedirs(dir)

    fh = logging.handlers.RotatingFileHandler(args.log_file, maxBytes=args.log_file_size, backupCount=args.log_file_count)
    fh.setFormatter(logging.Formatter(args.log_format))
    fh.setLevel(args.log_level)
    logging.getLogger().addHandler(fh)

logger = logging.getLogger("o-namazu")
logger.info(args)

# Be quiet schedule-lib !!
ScheduleLogLevel = "WARN"
if args.log_level == "DEBUG":
    ScheduleLogLevel = "DEBUG"
logging.getLogger('schedule').setLevel(ScheduleLogLevel)
logger.info(f"Change 'schedule-lib' log level to {ScheduleLogLevel}")
# logging.getLogger('schedule').propagate = False

# -------------------------------------------------


def sample_handler(ev):
    logger.info(f"{ev.src_path}@{ev.created_at}")
    path = Path(ev.src_path)

    if "mqtt" in ev.config:
        mqtt_config = ev.config["mqtt"]
        mqtt_config = dict(config.DefaultConfig_MQTT, **mqtt_config)

        host = mqtt_config["host"]
        port = mqtt_config["port"]
        topic = mqtt_config["topic"]
        format = mqtt_config["format"]
        length = mqtt_config["length"]

        if format == "csv":
            payload = csv_handler.read(path, ev.config)
            line_count = len(payload.strip().split("\n"))
            logger.info(f"CSV Payload is {line_count} lines.")
            payloads = csv_handler.split(payload, length)
        elif format == "text":
            payload = text_handler.read(path, ev.config)
            line_count = len(payload.strip().split("\n"))
            logger.info(f"Text Payload is {line_count} lines.")
            payloads = text_handler.split(payload, length)
        else:
            logger.error(f"Unsupported format `{format}`, `{path}` has not sent.")
            return

        if len(payloads) <= 0:
            logger.info(f"No Payload found. Empty file ? `{path}` was not sent")
            return

        client_id = f"o-namazu_{os.uname()[1]}_{path}"
        mqtt_sender.publish(payloads, client_id, topic, host, port)

        lines = reduce(add, [len(payload.strip().split("\n")) for payload in payloads])
        bytes = reduce(add, [len(payload) for payload in payloads])

        logger.info(f"MQTT sent `{path}` done. {len(payloads)} messages, {lines} lines, {bytes} bytes.")


# -------------------------------------------------


config_map = config.create_config_map(Directory)
logger.info(f'config_map={config_map}')

if len(config_map) == 0:
    logger.error("No config file found. Please see README.md")
    sys.exit()

for path, json in config_map.items():
    logger.info(f'Watching: {path}/{json["pattern"]}')

w = watcher.NamazuWatcher(Directory, config_map, sample_handler)
w.start()

sweep_interval = int(args.archive_interval)
schedule.every(sweep_interval).seconds.do(lambda: sweeper.sweep(config_map))

logger.info(f"Observe started '{Directory}'")
logger.info("Press 'Ctrl-c' to exit")
try:
    while True:
        w.wait(1)
        schedule.run_pending()
except KeyboardInterrupt:
    w.stop()
