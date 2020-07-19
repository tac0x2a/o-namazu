FROM python:3.8.3-slim

LABEL maintainer="tac@tac42.net"

# o-namazu is data collector that traverse specified directories.

RUN mkdir /o-namazu
ADD README.md onamazu.py requirements.txt /o-namazu/
ADD onamazu /o-namazu/onamazu
WORKDIR /o-namazu

RUN apt-get update && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends gcc g++ musl-dev \
    && pip install -r requirements.txt \
    && apt-get clean \
    && apt-get autoclean \
    && apt-get autoremove \
    && rm -rf /tmp/* /var/tmp/* \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/lib/apt/lists/*


################
# Environments #
################

# Target directory to observe by o-namazu
ENV TARGET_DIR /data

# Log level [DEBUG, INFO, WARN, ERROR]
ENV LOG_LEVEL INFO

#  Log format by 'logging' package
ENV LOG_FORMAT "[%(levelname)s] %(asctime)s | %(pathname)s(L%(lineno)s) | %(message)s"

# Log file name
ENV LOG_FILE onamazu.log

# Count of file files kept
ENV LOG_FILE_COUNT 1000

# Size of each log file
ENV LOG_FILE_SIZE  1000000

RUN mkdir /logs
VOLUME /logs

RUN mkdir /data
VOLUME /data

ENTRYPOINT python ./onamazu.py ${TARGET_DIR} \
    --log-level ${LOG_LEVEL} \
    --log-file /logs/`cat /etc/hostname`/${LOG_FILE} \
    --log-format "${LOG_FORMAT}" \
    --log-file-count ${LOG_FILE_COUNT}\
    --log-file-size ${LOG_FILE_SIZE}
