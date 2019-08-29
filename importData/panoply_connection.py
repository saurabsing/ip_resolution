# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""A simple Database Connector class"""

import sys
import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import linecache
from service_config import ServiceConfig


def printException():
    """This class gives a data connector"""

    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print('EXCEPTION IN ( LINE {} "{}"): {}'.format(lineno, line.strip(), exc_obj))


class PanoplyConnector:
    """This class gives a panoply connector."""

    def __init__(self):
        """Initialize instance of IPAddressResolver."""
        self.logger = logging.getLogger('ip_resolution')
        self.logger.info('Initializing IpResolver')
        self.config = ServiceConfig().getConfig()
        self.process_data = True
        username = self.config['panoplydatabase']['user']
        password = self.config['panoplydatabase']['password']
        database = self.config['panoplydatabase']['database']
        host = self.config['panoplydatabase']['host']
        port = self.config['panoplydatabase']['port']
        self.connection_string = 'postgresql://' + username + ':' + password + '@' + host + ':' + port + '/' + database
        try:
            self.connection_panoply = create_engine(self.connection_string, echo=False)
        except Exception as ex:
            self.logger.info('Issue with panoply connection:' + ex)
            printException()

    def getConnection(self):
        """Get a single panoply connection."""
        return self.connection_panoply

    def getPandasDataFrame(self, query):
        """Initialize instance of IPAddressResolver."""
        try:
            dataframe_ip_address = pd.read_sql_query(query, self.connection_panoply)
            return dataframe_ip_address
        except Exception as ex:
            self.logger.info("Issue with panoply connection:" + ex)
            printException()

    def cleanup(self):
        """Perform cleanup of resources for Data connector"""
        self.connection_panoply.close()
