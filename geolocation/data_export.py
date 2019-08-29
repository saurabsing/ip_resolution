# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""A simple Database Connector class"""

from __future__ import annotations

from abc import ABC, abstractmethod

import s3fs
import time
import logging
from datetime import date
import psycopg2
from service_config import ServiceConfig
import psutil
import os

class DataWriter(ABC):
    """This class gives a pandas dataframe writer."""

    @abstractmethod
    def saveData(self, dataframe_pandas):
        pass

    @abstractmethod
    def appendData(self, dataframe_pandas, processNo):
        pass


class S3Writer(DataWriter):
    """This class gives a S3 writer."""

    def __init__(self):
        """Initialize instance of IPAddressResolver."""
        self.config = ServiceConfig().getConfig()
        self.filePath = self.config['storageDetails']['filePath']
        self.fileName = self.config['storageDetails']['fileName']
        self.fileExtension = self.config['storageDetails']['fileExtension']
        self.directoryName = date.today().strftime("%m/%d/%y").replace("/", "_")
        self.awsKey = self.config['storageDetails']['awsKey']
        self.secretKey = self.config['storageDetails']['secretKey']
        self.s3file_url = 's3://' + str(self.filePath) + '/' + str(self.directoryName) \
                          + '/' + str(self.fileName) + str(self.fileExtension)
        self.fileWriter = s3fs.S3FileSystem(self.awsKey, self.secretKey)

    def getFileUrl(self, num=0):
        """Initialize instance of IPAddressResolver."""
        return 's3://' + str(self.filePath) + '/' + str(self.directoryName) \
            + '/' + str(self.fileName) + str(num) + str(self.fileExtension)

    def saveData(self, dataframe_pandas):
        """Initialize instance of IPAddressResolver."""
        try:
            data_bytes = dataframe_pandas[['ipaddress', 'ipaddress_stripped', 'country', 'city', 'region']] \
                .to_csv(None, index=False).encode()
            with self.fileWriter.open(self.s3file_url, mode='wb', block_size=None, acl='public-read') as pointer:
                pointer.write(data_bytes)
                pointer.close()
            print("Finished writing in S3")
        except Exception as ex:
            print(ex)
            raise

    def appendData(self, dataframe_pandas, processNo):
        """Initialize instance of IPAddressResolver."""
        try:
            seconds = time.time()
            print("Started the file append operation %s at %s " % (str(processNo), time.time()))
            s3file_url = self.getFileUrl(processNo)
            fileExists = self.fileWriter.exists(s3file_url)
            if fileExists:
                data_bytes = dataframe_pandas[['ipaddress', 'ipaddress_stripped', 'country', 'city', 'region']] \
                    .to_csv(None, header=False, index=False).encode()
            else:
                data_bytes = dataframe_pandas[['ipaddress', 'ipaddress_stripped', 'country', 'city', 'region']] \
                    .to_csv(None, header=True, index=False).encode()
            with self.fileWriter.open(s3file_url, mode='ab', block_size=None, acl='public-read') as pointer:
                pointer.write(data_bytes)
                pointer.close()
            print("Ended file append operation %s in %s " % (str(processNo), time.time() - seconds))
        except Exception as ex:
            print(ex)
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Perform cleanup of resources for Data connector"""
        self.fileWriter = None
        print('cleaned file')


class PanoplyWriter(DataWriter):
    """This class gives a S3 writer."""

    def __init__(self, num=0):
        """Initialize instance of IPAddressResolver."""
        self.config = ServiceConfig().getConfig()
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
        self.copy_command = """truncate """ + self.tableName + """ ; copy """ + self.tableName + """ from '""" + self.s3file_url + """'
        access_key_id  '""" + self.awsKey + """'
        secret_access_key '""" + self.secretKey + """'
        region '""" + self.region + """'
        ignoreheader 1
        null as 'NA'
        removequotes
        delimiter ',';"""

    def saveData(self):
        """write comments"""
        cursor = self.connection.cursor()
        cursor.execute(self.copy_command)
        self.connection.commit()
        cursor.close()
        self.connection.close()
        print('finished copying to redshift')

    def appendData(self):
        """write comments"""
        self.saveData()
