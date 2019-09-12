# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that provides any utility functions in ip resolution job."""


from data_export import S3Writer
from datetime import date
from job_config import JobConfig
import linecache
import sys
import logging
import os


def getlogger(loggerName, fileName, processNo=0):
    """Get a logger."""
    logger = logging.getLogger(loggerName)
    process_no_str = 'main' if processNo == 0 else str(processNo)
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        log_handler = logging.FileHandler(os.path.join('/', 'ipaddress_logs', fileName + "_" + process_no_str + '.log'))
        log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(log_handler)
    return logger


def print_exception():
    """Print an exception."""
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    return 'EXCEPTION IN ( LINE {} "{}"): {}'.format(lineno, line.strip(), exc_obj)


def splitdataframe1(dataframe):
    """Alternative approach, but not correct."""
    size = dataframe.shape[0]
    config = JobConfig().getconfig()
    max_rows = size // config['processConfig']['noOfParallelProcess']
    print(max_rows)
    dataframes = []
    while size > max_rows:
        top = dataframe.loc[0:max_rows-1]
        dataframes.append(top)
        dataframe = dataframe.loc[max_rows:size-1]
        size = dataframe.shape[0]
    else:
        dataframes.append(dataframe)
    return dataframes


def split_dataframe(dataframe):
    """Split a Pandas dataframe in sets."""
    size = dataframe.shape[0]
    config = JobConfig().getconfig()
    max_rows = size // config['processConfig']['noOfParallelProcess']
    list_df = [dataframe[i:i+max_rows] for i in range(0, dataframe.shape[0], max_rows)]
    return list_df


def check_s3path(config):
    """Clean existing S3 path in case of rerun."""
    s3writer = S3Writer()
    filePath = config['storageDetails']['filePath']
    directoryName = date.today().strftime("%m/%d/%y").replace("/", "_")
    s3Directory = 's3://' + str(filePath) + '/' + str(directoryName)
    dirExists = s3writer.fileWriter.exists(s3Directory)
    if dirExists:
        s3writer.fileWriter.rm(s3Directory, recursive=True)
    else:
        s3writer.fileWriter.mkdir(s3Directory, acl='public-read')
    s3writer = None
