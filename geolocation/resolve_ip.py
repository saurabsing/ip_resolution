# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that resolves the IP Address to physical address.

This module runs parallel processes on dataset chunks, writes physical addresses
data to S3 loaction and sends data to external system(panoply)
GeoService to use for ip address resolution and writer to external system are the
decoupled functionalites.
Datasets are used in form of pandas dataframe and using S3 as intermediate layer
are ingrained factors in design.

"""


import multiprocessing as processes
import utility_functions as utility
from data_import import PanoplyImport, DataImport
from geo_service import IPDataService, GeoService
from data_export import S3Writer, PanoplyWriter, DataExport
from ip_resolution import IPResolver
from job_config import JobConfig
import time


def process_chunks(config, ipResolver, geoservice: GeoService, datareader: DataImport, datawriter: DataExport) -> bool:
    """Triggers parallel processes.

    Resolves IP in pandas dataframes chunks and stores ip resolved data in S3.
    """
    # Initialize logging
    logger = utility.getlogger('ip_resolution', 'ip_resolution')
    seconds = time.time()
    try:
        query_panoply = config["panoplydatabase"]["readQuery"]
        for dataframe_ip_address in datareader.getbatch_pandas_dataframe(query_panoply):
            dataframes = utility.split_dataframe(dataframe_ip_address)
            processNo = 0
            processList = []
            for frame in enumerate(dataframes):
                processNo = processNo + 1
                process_ipresolve = processes.Process(target=ipResolver.resolve_ipaddress,
                                                      args=(frame[1], geoservice, datawriter, processNo))
                processList.append(process_ipresolve)
                process_ipresolve.start()
                logger.info('processNo-' + str(process_ipresolve.pid))
            for p in processList:
                p.join()
                # print(str(p.exitcode))
    except Exception as ex:
        logger.info('Issue in fetching data from Panoply:' + str(ex))
        logger.error(utility.print_exception())
        return False
    logger.info("Finished the batch job in %s seconds" % str((time.time() - seconds) // 1))
    return True


if __name__ == "__main__":
    logger = utility.getlogger('ip_resolution', 'ip_resolution')
    logger.info('Starting ip resolution job')
    config = JobConfig().getconfig()
    utility.check_s3path(config)
    ip_resolver = IPResolver()
    panoplyreader = PanoplyImport()
    datawriter = S3Writer()
    ipdata_geoservice = IPDataService()
    if process_chunks(config, ip_resolver, ipdata_geoservice, panoplyreader, datawriter):
        panoplywriter = PanoplyWriter()
        panoplywriter.save_data()
