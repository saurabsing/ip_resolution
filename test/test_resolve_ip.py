# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that retrieves physical address corresponding to the IP address."""


import logging
import unittest
import sys
sys.path.append('C://ipaddress//geolocation')
# sys.path.append('..//..//')
import resolve_ip as ResolveIp  # noqa: E402
import utility_functions as utility  # noqa: E402
from data_import import PanoplyImport  # noqa: E402
from geo_service import IPDataService  # noqa: E402
from data_export import S3Writer  # noqa: E402
from ip_resolution import IpResolver  # noqa: E402
from job_config import JobConfig  # noqa: E402


class TestResolveIp(unittest.TestCase):
    """Unit Tests for batch job."""

    def test_case_resolveIp(self):
        """..."""
        config = JobConfig().getConfig()
        utility.checkS3Path(config)
        ipResolver = IpResolver()
        panoplyreader = PanoplyImport()
        datawriter = S3Writer()
        ipdata_geoservice = IPDataService()
        self.assertTrue(ResolveIp.process_chunks(config, ipResolver, ipdata_geoservice, panoplyreader, datawriter))

    def test_case_panoplyImport(self):
        """..."""
        connector = PanoplyImport()
        query_panoply = 'select distinct ipaddress from activity_us (nolock) where ipaddress is not null limit 20'
        for dataframe_ip_address in connector.getPandasDataFrameBatch(query_panoply):
            dataframes = utility.splitDataframe(dataframe_ip_address)
            self.assertTrue(len(dataframes) > 0)
        print('test')

    def test_case_s3export(self):
        """..."""
        connector = PanoplyImport()
        query_panoply = 'select distinct ipaddress from activity_us (nolock) where ipaddress is not null limit 20'
        for dataframe_ip_address in connector.getPandasDataFrameBatch(query_panoply):
            dataframes = utility.splitDataframe(dataframe_ip_address)
            self.assertTrue(len(dataframes) > 0)
        print('test')

if __name__ == '__main__':
    # Setup logging
    logger = logging.getLogger('ip_resolution_test')
    logger.setLevel(logging.INFO)

    log_handler = logging.FileHandler('\\ipaddress_logs\\ip_resolution_tests.log')
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_handler)

    logger.info('Running ip resolution Test cases')
    unittest.main()
