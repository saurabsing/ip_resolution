# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that retrieves physical address corresponding to the IP address."""


import logging
import multiprocessing as processes
import utility_functions as utility
from panoply_connection import PanoplyConnector
from geo_service import IPDataService
from data_export import S3Writer, PanoplyWriter
from ip_resolution import IpResolver
from service_config import ServiceConfig
import time
import psutil
import os


def get_process_memory(pid):
    process = psutil.Process(pid)
    return process.memory_info().rss


def run():
    """Trigger run of Consumer to pull data from stream and persist in database."""
    # Setup logging
    logger = logging.getLogger('ip_resolution')
    logger.setLevel(logging.INFO)

    log_handler = logging.FileHandler('C:\\ipaddress_logs\\ip_resolution.log')
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_handler)

    logger.info('Starting service ip resolution service')
    seconds = time.time()

    config = ServiceConfig().getConfig()
    query_panoply = config["panoplydatabase"]["readQuery"]

    # utility.cleanS3Path(config)
    ipResolver = IpResolver()
    connector = PanoplyConnector()

    s3writer = S3Writer()
    geoservice = IPDataService()

    for dataframe_ip_address in connector.getPandasDataFrameBatch(query_panoply):
        dataframes = utility.splitDataframe(dataframe_ip_address)
        processNo = 0
        processList = []
        for frame in enumerate(dataframes):
            processNo = processNo + 1
            process_ipresolve = processes.Process(target=ipResolver.resloveIp, args=(frame[1], geoservice, s3writer, processNo))
            processList.append(process_ipresolve)
            process_ipresolve.start()
            print('processNo-' + str(process_ipresolve.pid))
        for p in processList:
            p.join()
            # print(str(p.exitcode))

    return True
    panoplywriter = PanoplyWriter()
    panoplywriter.saveData()
    logger.info('Batch Job completed')
    print("Finished the main batch job in %s seconds" % str((time.time() - seconds) // 1))
    return True


if __name__ == "__main__":
    run()
