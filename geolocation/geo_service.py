# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that provides classes for fetching loaction from external service.

This module has an abstract interface for GeoService classes and specific geolocation
class to fetch physical address from ip address.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
import requests
from job_config import JobConfig
import utility_functions as utility


class GeoService(ABC):
    """This class acts as an interface to declare abstract methods for geoservice."""

    @abstractmethod
    def fetch_geolocation(self) -> {}:
        """Fetch address object from third party service."""
        pass


class IPDataService(GeoService):
    """Provides implementation of retrieving location from ip address."""

    def __init__(self, processNo=0):
        """Initialize IP Data API object connection."""
        self.config = JobConfig().getconfig()
        self.processNo = processNo
        self.logger = utility.getlogger('ip_resolution', 'ip_resolution')
        url = self.config['geoservice']['url']
        apikey = self.config['geoservice']['apikey']
        self.connection_url = url.replace("userkey", apikey)

    def setlogger(self, processNo):
        """Get Logger for use in a python child process."""
        self.logger = utility.getlogger('ipdataservice', 'ipdataservice', processNo)

    def fetch_geolocation(self, ipaddress) -> {}:
        """Fetch Location as json object."""
        try:
            url = self.connection_url.replace("ipaddress", ipaddress)
            req = requests.get(url)
        except requests.exceptions.HTTPError as ex:
            self.logger.error("Http Error:" + str(ex))
        except requests.exceptions.ConnectionError as ex:
            self.logger.error("Error Connecting:" + str(ex))
        except requests.exceptions.Timeout as ex:
            self.logger.error("Timeout Error:" + str(ex))
        except requests.exceptions.RequestException as ex:
            self.logger.error('Issue with requesting location:' + str(ex))
        except Exception as ex:
            self.logger.info('Issue with requesting the Geoservice for location:' + str(ex))
            self.logger.error(utility.print_exception())
        return req.json()

    def cleanup(self):
        """Perform cleanup of resources."""
        self.connection_url = None
