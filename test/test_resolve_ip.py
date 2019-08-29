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
import unittest
import sys
import resolve_ip as ResolveIp

sys.path.append('../../')



class TestResolveIp(unittest.TestCase):
    """Unit Tests for SkillComputation."""

    logger = logging.getLogger('ip_resolution')

    def test_case1(self):
        """..."""
        self.assertTrue(ResolveIp.run())


if __name__ == '__main__':
    # Setup logging
    logger = logging.getLogger('ip_resolution')
    logger.setLevel(logging.INFO)

    log_handler = logging.FileHandler('C:\\ipaddress_logs\\ip_resolution.log')
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_handler)

    logger.info('Running ip resolution Test cases')
    unittest.main()
