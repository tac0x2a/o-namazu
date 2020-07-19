#!/usr/bin/env python

import argparse
import os
import logging
import schedule
from onamazu import config, watcher, csv_handler, text_handler, mqtt_sender, sweeper

from pathlib import Path

# -------------------------------------------------
# Argument Parsing
parser = argparse.ArgumentParser(description='Observe file modification recursively, and callback')
parser.add_argument('observe_directory', help='Target to observe directory')  # Required

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


def sample_handler(ev):
    logger.info(f"{ev.src_path}@{ev.created_at}")
    path = Path(ev.src_path)
    logger.info(f"{path.stat().st_size} bytes")

    if "mqtt" in ev.config:
        mqtt_config = ev.config["mqtt"]
        host = mqtt_config["host"]
        port = mqtt_config["port"]
        topic = mqtt_config["topic"]
        format = mqtt_config["format"]

        mqtt = mqtt_sender.MQTT_Sender()
        mqtt.connect(host, port)

        if format == "csv":
            payload = csv_handler.read(path, ev.config)
        elif format == "text":
            payload = text_handler.read(path, ev.config)
        else:
            logger.error(f"Unsupported format `{format}`, `{path}` has not sent.")
            return

        if len(payload) <= 0:
            logger.info(f"Payload is empty. `{path}` was not sent")
            return

        line_count = len(payload.strip().split("\n"))
        logger.info(f"MQTT send Start `{path}` as `{format}` ({len(payload)} bytes, {line_count} lines) will be sent")
        mqtt.send(payload, topic)
        logger.info(f"MQTT send Done `{path}`")


# -------------------------------------------------


config_map = config.create_config_map(Directory)
logger.info(f'config_map={config_map}')

for path, json in config_map.items():
    logger.info(f'Watching: {path}/{json["pattern"]}')

w = watcher.NamazuWatcher(Directory, config_map, sample_handler)
w.start()

schedule.every(1).minutes.do(lambda: sweeper.sweep(config_map))

logger.info(f"Observe started '{Directory}'")
logger.info(f"Press 'Ctrl-c' to exit")
try:
    while True:
        w.wait(1)
        schedule.run_pending()
except KeyboardInterrupt:
    w.stop()
