#!/usr/bin/env python

import argparse
import logging
import logging.handlers
import os
from onamazu.onamazu import ONamazu

# Argument Parsing
parser = argparse.ArgumentParser(description='Observe file modification recursively, and callback')
parser.add_argument('observe_directory', help='Target to observe directory')  # Required
parser.add_argument('--archive_interval', help='Traverse interval seconds to judge the file should be archive [sec]', type=int, default=60)

parser.add_argument('--log-level', help='Log level', choices=['DEBUG', 'INFO', 'WARN', 'ERROR'], default='INFO')
parser.add_argument('--log-format', help='Log format by \'logging\' package', default='[%(levelname)s] %(asctime)s | %(pathname)s(L%(lineno)s) | %(message)s')  # Optional
parser.add_argument('--log-file', help='Log file path')
parser.add_argument('--log-file-count', help='Log file keep count', type=int, default=1000)
parser.add_argument('--log-file-size', help='Size of each log file', type=int, default=1000000)  # default 1MB
args = parser.parse_args()


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

# -----------------------------------------------
onamazu = ONamazu(args.observe_directory, args.archive_interval, logger)
try:
    while True:
        onamazu.click()
except KeyboardInterrupt:
    logger.info("Interrupted. shutting down...")
    onamazu.stop()

except Exception as e:
    logger.error(e, exc_info=e)
    logger.error("It's error! Shutting down...")
