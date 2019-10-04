# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that retrieves physical address corresponding to the IP address."""


import logging
import unittest
import socket
import sys
import multiprocessing as processes
sys.path.append('C://ipaddress//geolocation')
# sys.path.append('..//..//')
import resolve_ip as ResolveIp  # noqa: E402
import utility_functions as utility  # noqa: E402
from data_import import PanoplyImport  # noqa: E402
from geo_service import IPDataService  # noqa: E402
from data_export import S3Writer, PanoplyWriter  # noqa: E402
from ip_resolution import IPResolver  # noqa: E402
from job_config import JobConfig  # noqa: E402
from datetime import datetime  # noqa: E402


class TestResolveIp(unittest.TestCase):
    """Unit Tests for batch job."""

    def test_case_panoplyImport_dataframes(self):
        """Test the import of dataframe from Panoply."""
        connector = PanoplyImport()
        config = JobConfig().getconfig()
        query_panoply = 'select distinct ipaddress from activity_us (nolock) where ipaddress is not null limit 40'
        for panda_df in connector.getbatch_pandas_dataframe(query_panoply):
            self.assertTrue(panda_df.shape[0] <= config['panoplydatabase']['chunksize'] and (panda_df['ipaddress'].iloc[0] is not None))
        print('Ran test_case_panoplyImport_dataframes test case')

    def test_case_dataframe_splitCheck(self):
        """Test the splitting of dataframe based on processes no."""
        connector = PanoplyImport()
        query_panoply = 'select distinct ipaddress from activity_us (nolock) where ipaddress is not null limit 40'
        for panda_df in connector.getbatch_pandas_dataframe(query_panoply):
            dataframes = utility.split_dataframe(panda_df)
            totalLen = 0
            for frame in enumerate(dataframes):
                totalLen = totalLen + frame[1].shape[0]
            self.assertEqual(totalLen, panda_df.shape[0])
        print('Ran test_case_dataframe_splitCheck test case')

    def test_case_fetch_ipdata_geolocation(self):
        """Test the geoservice GET api response."""
        ipdata_geoservice = IPDataService()
        hostname = socket.gethostname()
        ipaddress = socket.gethostbyname(hostname)
        addressObject = ipdata_geoservice.fetch_geolocation(ipaddress)
        self.assertIn('country_code', addressObject)
        self.assertIn('region', addressObject)
        self.assertIn('city', addressObject)
        print('Ran test_case_fetch_ipdata_geolocation test case')

    def test_case_s3_writer(self):
        """Test file write to S3 bucket."""
        config = JobConfig().getconfig()
        utility.check_s3path(config)
        panoplyreader = PanoplyImport()
        datawriter = S3Writer()
        # print(dataframe_results)
        query_panoply = 'select distinct ipaddress from activity_us (nolock) where ipaddress is not null limit 10'
        dataframe_results = panoplyreader.get_pandas_dataframe(query_panoply)
        datawriter.append_data(dataframe_results, 0)
        datawriter = S3Writer()
        s3file_url = datawriter.getfileurl(0)
        with datawriter.fileWriter.open(s3file_url, mode='rb') as pointer:
            file_bytes = pointer.read()
        data_bytes = dataframe_results[['ipaddress']].to_csv(None, header=False, index=False).encode()
        self.assertEqual(file_bytes[11:], data_bytes)
        print('Ran test_case_s3_writer test case')

    def test_case_save_resolved_ipdata(self):
        """Test the process of ip resolution and file placement in S3."""
        config = JobConfig().getconfig()
        utility.check_s3path(config)
        ipResolver = IPResolver()
        panoplyreader = PanoplyImport()
        datawriter = S3Writer()
        ipdata_geoservice = IPDataService()
        query_panoply = config["panoplydatabase"]["readQuery"]
        for dataframe_ip_address in panoplyreader.getbatch_pandas_dataframe(query_panoply):
            dataframes = utility.split_dataframe(dataframe_ip_address)
            processNo = 0
            processList = []
            for frame in enumerate(dataframes):
                processNo = processNo + 1
                process_ipresolve = processes.Process(target=ipResolver.resolve_ipaddress,
                                                      args=(frame[1], ipdata_geoservice, datawriter, processNo))
                processList.append(process_ipresolve)
                process_ipresolve.start()
                logger.info('processNo-' + str(process_ipresolve.pid))
            for p in processList:
                p.join()
        for i in range(1, config['processConfig']['noOfParallelProcess']):
            s3file_url = datawriter.getfileurl(i)
            self.assertTrue(datawriter.fileWriter.exists(s3file_url))
        print('Ran test_case_save_resolved_ipdata test case')

    def test_case_ipresolution_pipeline(self):
        """Test the complete flow of ip resolution and result load in panoply."""
        config = JobConfig().getconfig()
        utility.check_s3path(config)
        ip_resolver = IPResolver()
        panoplyreader = PanoplyImport()
        datawriter = S3Writer()
        ipdata_geoservice = IPDataService()
        if ResolveIp.process_chunks(config, ip_resolver, ipdata_geoservice, panoplyreader, datawriter):
            panoplywriter = PanoplyWriter()
            panoplywriter.save_data()
        query_panoply = 'select max(createdAt) as created from test_sp_ipaddress_parsed_us (nolock) limit 1'
        df = panoplyreader.get_pandas_dataframe(query_panoply)
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y")
        df_string = df['created'].iloc[0]
        self.assertTrue(df_string[0:10] == dt_string)
        print('Ran test_case_ipresolution_pipeline test case')


if __name__ == '__main__':
    # Setup logging
    logger = logging.getLogger('ip_resolution_test')
    logger.setLevel(logging.INFO)

    log_handler = logging.FileHandler('\\ipaddress_logs\\ip_resolution_tests.log')
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_handler)

    logger.info('Running ip resolution Test cases')
    unittest.main()
