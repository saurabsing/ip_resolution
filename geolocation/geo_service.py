# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""A simple Database Connector class"""

from __future__ import annotations

from abc import ABC, abstractmethod

import requests
import sys
import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import linecache
from service_config import ServiceConfig


class GeoService(ABC):
    """This class gives a geoservice connector."""

    @abstractmethod
    def fetchGeoLocation(self) -> {}:
        pass


class IPDataService(GeoService):
    """This class gives a geoservice connector."""

    def __init__(self):
        """Initialize instance of IPAddressResolver."""
        self.config = ServiceConfig().getConfig()
        url = self.config['geoservice']['url']
        apikey = self.config['geoservice']['apikey']
        self.connection_url = url.replace("userkey", apikey)

    def fetchGeoLocation(self, ipaddress) -> {}:
        """Initialize instance of IPAddressResolver."""

        url = self.connection_url.replace("ipaddress", ipaddress)
        req = requests.get(url)
        return req.json()

    def cleanup(self):
        """Perform cleanup of resources for Data connector"""
        self.connection_panoply.close()
