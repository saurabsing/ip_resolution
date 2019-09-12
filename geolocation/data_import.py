# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that provides classes for importing data for external or AWS service.

This module has an abstract interface for Data Import classes and specific import
class from Panoply datawarehouse.
"""


from __future__ import annotations
from abc import ABC, abstractmethod
import utility_functions as utility
import pandas as pd
import sys
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from job_config import JobConfig


class DataImport(ABC):
    """This class acts as an interface to declare abstract methods for data import."""

    @abstractmethod
    def get_pandas_dataframe(self, query):
        """Get data in a single batch."""
        pass

    @abstractmethod
    def getbatch_pandas_dataframe(self, query):
        """Get data in chunks."""
        pass

    @abstractmethod
    def cleanup(self):
        """To clean resources."""
        pass


class PanoplyImport(DataImport):
    """Provides implementation of retrieving data as Pandas dataframe from Panoply."""

    def __init__(self):
        """Initialize Panoply connection."""
        self.config = JobConfig().getconfig()
        self.logger = utility.getlogger('ip_resolution', 'ip_resolution')
        username = self.config['panoplydatabase']['user']
        password = self.config['panoplydatabase']['password']
        db = self.config['panoplydatabase']['database']
        host = self.config['panoplydatabase']['host']
        port = self.config['panoplydatabase']['port']
        self.connection_url = 'postgresql://' + str(username) + ':' + str(password) + '@' + str(host) + ':' + str(port) + '/' + str(db)
        self.readQuery = self.config['panoplydatabase']['readQuery']
        self.chunksize = self.config['panoplydatabase']['chunksize']
        try:
            self.connection_panoply = create_engine(self.connection_url, echo=False)
            self.logger.info('Initialized Panoply connection')
        except Exception as ex:
            self.logger.info('Issue with panoply connection:' + str(ex))
            self.logger.error(utility.print_exception())

    def setlogger(self, processNo):
        """Get Logger for use in a python child process."""
        self.logger = utility.getlogger('panoply_connector', 'panoplyconnector', processNo)

    def getconnection(self):
        """Get single panoply connection."""
        return self.connection_panoply

    def get_pandas_dataframe(self, query):
        """Get complete data as pandas dataframe."""
        try:
            dataframe_ip_address = pd.read_sql_query(query, self.getconnection())
            return dataframe_ip_address
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            self.logger.info('SQLAlchemyError:' + error)
            self.logger.error(utility.print_exception())
            sys.exit()
        except Exception as ex:
            self.logger.info('Issue in fetching data from Panoply:' + str(ex))
            self.logger.error(utility.print_exception())
            sys.exit()

    def getbatch_pandas_dataframe(self, readQuery):
        """Get data in chunks from pandas dataframe."""
        try:
            dataframe_iterator = pd.read_sql_query(readQuery, self.getconnection(), chunksize=self.chunksize)
            self.logger.info('Fetched new chunk from Panoply table')
            for dataframe_ip_batch in dataframe_iterator:
                # print(dataframe_ip_batch)
                yield dataframe_ip_batch
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            self.logger.info('SQLAlchemyError:' + error)
            self.logger.error(utility.print_exception())
            sys.exit()
        except Exception as ex:
            self.logger.info('Issue in fetching data from Panoply:' + str(ex))
            self.logger.error(utility.print_exception())
            sys.exit()

    def cleanup(self):
        """Perform cleanup of resources."""
        self.connection_panoply.close()
