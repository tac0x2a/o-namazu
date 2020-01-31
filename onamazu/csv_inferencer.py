#!/usr/bin/env python

import time
import pandas as pd
import logging

logger = logging.getLogger("csv_inferencer")


def type_csv(src_path: str):
    before = time.time()
    df = pd.read_csv(src_path)
    types = {k: str(v) for k, v in df.dtypes.items()}
    logger.debug(df.head())
    logger.debug(df.describe())
    logger.info(types)
    logger.debug(f"parse {src_path} in {time.time() - before} seconcs")

    return types


def convert_json_list(src_path: str):
    before = time.time()
    df = pd.read_csv(src_path)

    json_list = df.to_json(orient='records', lines=True).splitlines()
    logger.debug(json_list[0])
    logger.info(f"dict_list {src_path} in {time.time() - before} seconcs")

    return json_list
