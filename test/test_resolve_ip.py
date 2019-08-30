# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that retrieves physical address corresponding to the IP address."""


import logging
import unittest
import sys
import os
sys.path.append('C://ipaddress//geolocation')
#sys.path.append('..//..//')
import resolve_ip as ResolveIp  # noqa: E402


class TestResolveIp(unittest.TestCase):
    """Unit Tests for SkillComputation."""

    def test_case1(self):
        """..."""
        self.assertTrue(ResolveIp.run())


if __name__ == '__main__':
    # Setup logging
    logger = logging.getLogger('ip_resolution_test')
    logger.setLevel(logging.INFO)

    log_handler = logging.FileHandler('C:\\ipaddress_logs\\ip_resolution_tests.log')
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(log_handler)

    logger.info('Running ip resolution Test cases')
    unittest.main()
