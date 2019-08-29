# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that retrieves physical address corresponding to the IP address."""

from data_export import S3Writer
from datetime import date
from service_config import ServiceConfig

def splitDataframe1(dataframe):
    """."""
    size = dataframe.shape[0]
    config = ServiceConfig().getConfig()
    max_rows = size // config['processConfig']['noOfParallelProcess']
    print(max_rows)
    dataframes = []
    while size > max_rows:
        top = dataframe.loc[0:max_rows-1]
        dataframes.append(top)
        print(size)
        dataframe = dataframe.loc[max_rows:size-1]
        print(dataframe)
        size = dataframe.shape[0]
    else:
        dataframes.append(dataframe)
    return dataframes


def splitDataframe(dataframe):
    """test."""
    size = dataframe.shape[0]
    config = ServiceConfig().getConfig()
    max_rows = size // config['processConfig']['noOfParallelProcess']
    list_df = [dataframe[i:i+max_rows] for i in range(0, dataframe.shape[0], max_rows)]
    return list_df


def cleanS3Path(config):
    """test."""
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
