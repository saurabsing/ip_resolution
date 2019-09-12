# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that provides classes for exporting data to external or AWS service.

This module has an abstract interface for Data Export classes and specific export
class to AWS S3 filesystem, Panoply datawarehouse.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
import s3fs
import time
from datetime import date
import psycopg2
from job_config import JobConfig
import utility_functions as utility
import sys


class DataExport(ABC):
    """This class acts as an interface to declare abstract methods for data export."""

    @abstractmethod
    def save_data(self, dataframe_pandas):
        """Save pandas data."""
        pass

    @abstractmethod
    def append_data(self, dataframe_pandas, processNo):
        """Append to pandas data to existing persisted data."""
        pass

    @abstractmethod
    def cleanup(self):
        """To clean resources."""
        pass


class S3Writer(DataExport):
    """Provides implementations of storing data in Pandas dataframe to AWS S3."""

    def __init__(self):
        """Initialize connection to S3."""
        self.config = JobConfig().getconfig()
        self.logger = utility.getlogger('ip_resolution', 'ip_resolution')
        self.filePath = self.config['storageDetails']['filePath']
        self.fileName = self.config['storageDetails']['fileName']
        self.fileExtension = self.config['storageDetails']['fileExtension']
        self.directoryName = date.today().strftime("%m/%d/%y").replace("/", "_")
        self.awsKey = self.config['storageDetails']['awsKey']
        self.secretKey = self.config['storageDetails']['secretKey']
        self.s3file_url = 's3://' + str(self.filePath) + '/' + str(self.directoryName) \
                          + '/' + str(self.fileName) + str(self.fileExtension)
        self.fileWriter = s3fs.S3FileSystem(self.awsKey, self.secretKey)

    def setlogger(self, processNo):
        """Get Logger for use in a python child process."""
        self.logger = utility.getlogger('s3writer', 's3writer', processNo)

    def getfileurl(self, num=0):
        """Get file url to existing file path."""
        return 's3://' + str(self.filePath) + '/' + str(self.directoryName) \
            + '/' + str(self.fileName) + str(num) + str(self.fileExtension)

    def save_data(self, dataframe_pandas):
        """Save pandas dataframe to S3."""
        try:
            data_bytes = dataframe_pandas[['ipaddress', 'ipaddress_stripped', 'country', 'city', 'region']] \
                .to_csv(None, index=False).encode()
            with self.fileWriter.open(self.s3file_url, mode='wb', block_size=None, acl='public-read') as pointer:
                pointer.write(data_bytes)
                pointer.close()
            self.logger.info("Finished writing in S3")
        except IOError as e:
            self.logger.info("I/O error({0}): {1}".format(e.errno, e.strerror))
            self.logger.error(utility.print_exception())
            sys.exit()
        except Exception as ex:
            self.logger.info('Issue with saving to S3:' + str(ex))
            self.logger.error(utility.print_exception())
            sys.exit()
        finally:
            self.cleanup()

    def append_data(self, dataframe_pandas, processNo):
        """Save pandas dataframe to S3 in append mode."""
        try:
            seconds = time.time()
            self.logger.info("Started the file append operation %s at %s " % (str(processNo), time.time()))
            s3file_url = self.getfileurl(processNo)
            fileExists = self.fileWriter.exists(s3file_url)
            self.logger.info('s3 files url:' + s3file_url)
            if fileExists:
                data_bytes = dataframe_pandas[['ipaddress', 'ipaddress_stripped', 'country', 'city', 'region']] \
                    .to_csv(None, header=False, index=False).encode()
            else:
                data_bytes = dataframe_pandas[['ipaddress', 'ipaddress_stripped', 'country', 'city', 'region']] \
                    .to_csv(None, header=True, index=False).encode()
            with self.fileWriter.open(s3file_url, mode='ab', block_size=None, acl='public-read') as pointer:
                pointer.write(data_bytes)
                pointer.close()
            self.logger.info("Ended file append operation %s in %s " % (str(processNo), time.time() - seconds))
        except IOError as e:
            self.logger.info("I/O error({0}): {1}".format(e.errno, e.strerror))
            self.logger.error(utility.print_exception())
            sys.exit()
        except Exception as ex:
            self.logger.info('Issue with saving to S3:' + str(ex))
            self.logger.error(utility.print_exception())
            sys.exit()
        finally:
            self.cleanup()

    def cleanup(self):
        """Perform cleanup of resources."""
        self.fileWriter = None
        self.logger.info('cleaned file writers')


class PanoplyWriter(DataExport):
    """Provides implementations of storing Pandas dataframe to Panoply datawarehouse."""

    def __init__(self):
        """Initialize panoply connection."""
        self.config = JobConfig().getconfig()
        self.logger = utility.getlogger('ip_resolution', 'ip_resolution')
        self.filePath = self.config['storageDetails']['filePath']
        self.fileName = self.config['storageDetails']['fileName']
        self.fileExtension = self.config['storageDetails']['fileExtension']
        self.directoryName = date.today().strftime("%m/%d/%y").replace("/", "_")
        self.awsKey = self.config['storageDetails']['awsKey']
        self.secretKey = self.config['storageDetails']['secretKey']
        self.s3file_url = 's3://' + str(self.filePath) + '/' + str(self.directoryName) + '/' + str(self.fileName)
        self.tableName = self.config['storageDetails']['tableName']
        self.region = self.config['storageDetails']['region']
        username = self.config['panoplydatabase']['user']
        password = self.config['panoplydatabase']['password']
        db = self.config['panoplydatabase']['database']
        host = self.config['panoplydatabase']['host']
        port = self.config['panoplydatabase']['port']
        self.connection = psycopg2.connect(user=username, password=password, host=host, port=port, database=db)
        self.write_command = """BEGIN; truncate """ + self.tableName + """ ; copy """ + self.tableName + """ from '""" + self.s3file_url + """'
        access_key_id  '""" + self.awsKey + """'
        secret_access_key '""" + self.secretKey + """'
        region '""" + self.region + """'
        ignoreheader 1
        null as 'NA'
        removequotes
        delimiter ','; COMMIT;"""
        self.append_command = """BEGIN; copy """ + self.tableName + """ from '""" + self.s3file_url + """'
        access_key_id  '""" + self.awsKey + """'
        secret_access_key '""" + self.secretKey + """'
        region '""" + self.region + """'
        ignoreheader 1
        null as 'NA'
        removequotes
        delimiter ','; COMMIT;"""

    def setlogger(self, processNo):
        """Get Logger for use in a python child process."""
        self.logger = utility.getlogger('panoplywriter', 'panoplywriter', processNo)

    def save_data(self, mode='w'):
        """Save data from S3 to Panoply table using copy command."""
        self.logger.info('Panoply file urls:' + self.s3file_url)
        if mode == 'a':
            command = self.append_command
        else:
            command = self.write_command
        try:
            cursor = self.connection.cursor()
            cursor.execute(command)
            self.connection.commit()
            self.logger.info('finished copying to redshift')
        except psycopg2.DatabaseError as e:
            self.logger.info("Database error({0}): {1}".format(e.errno, e.strerror))
            self.logger.error(utility.print_exception())
            sys.exit()
        except Exception as ex:
            self.logger.info('Issue with copying from S3 to Panoply redshift:' + str(ex))
            self.logger.error(utility.print_exception())
            sys.exit()
        finally:
            cursor.close()
            self.cleanup()

    def append_data(self):
        """Append data from S3 to Panoply table using copy command."""
        self.saveData(mode='a')

    def cleanup(self):
        """Perform cleanup of resources."""
        self.connection.close()
        self.logger.info('closed panoply writer')
