# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that retrieves physical address corresponding to the IP address."""


import logging
import time
from service_config import ServiceConfig
import os
import psutil


class IpResolver:
    """Initialize instance of IP Resolver."""

    def __init__(self):
        """Initialize instance of IPAddressResolver."""
        self.logger = logging.getLogger('ip_resolution')
        self.logger.info('Initializing IpResolver')
        self.config = ServiceConfig().getConfig()

    def preprocess(self, dataframe_ip_address):
        """Initialize instance of ProcessingScheduler."""
        try:
            df = dataframe_ip_address
            df['ipaddress_stripped'] = df['ipaddress'].apply(lambda x: x.strip('::ffff:'))
            df.assign(city='', country='', region='')
            return df
        except Exception as ex:
            print(ex)
            raise

    def postprocess(self, dataframe_ip_address):
        """Initialize instance of ProcessingScheduler."""
        try:
            df = dataframe_ip_address
            df['country'] = df['country'].apply(lambda x: str(x).replace("'", "") if x else x)
            df['city'] = df['city'].apply(lambda x: str(x).replace("'", "") if x else x)
            df['region'] = df['region'].apply(lambda x: str(x).replace("'", "") if x else x)
            return df
        except Exception as ex:
            print(ex)
            raise

    def parseJsonAddress(self, addressObject):
        """Initialize instance of ProcessingScheduler."""
        try:
            country, region, city = '', '', ''
            if 'country_code' in addressObject:
                country = addressObject["country_code"]
            if 'region' in addressObject:
                region = addressObject["region"]
            if 'city' in addressObject:
                city = addressObject["city"]
        except KeyError:
            city = ''
            region = ''
            country = ''
        return country, region, city

    def getIpInformation(self, dataframe_ip_address, geocoding_service):
        """Initialize instance of ProcessingScheduler."""
        try:
            df = dataframe_ip_address
            for ind, row_series in df.iterrows():
                try:
                    ipaddress = df['ipaddress_stripped'][ind]
                    addressObject = geocoding_service.fetchGeoLocation(ipaddress)
                    country, region, city = self.parseJsonAddress(addressObject)
                    df.at[ind, 'country'] = country
                    df.at[ind, 'region'] = region
                    df.at[ind, 'city'] = city
                    print(addressObject)
                except Exception as ex:
                    print(ex)
            return df
        except Exception as ex:
            print(ex)
            raise

    def resloveIp(self, dataframe_ip_address, geocoding_service, s3writer, processNo):
        """Initialize instance of ProcessingScheduler."""
        try:
            seconds = time.time()
            print("Started the process %s at %s " % (str(processNo), time.time()))
            dataframe_ip_preprocessed = self.preprocess(dataframe_ip_address)
            dataframe_ip_processed = self.getIpInformation(dataframe_ip_preprocessed, geocoding_service)
            dataframe_results = self.postprocess(dataframe_ip_processed)
            print(dataframe_results)
            # s3writer.appendData(dataframe_results, processNo)
            process = psutil.Process(os.getpid())
            # print('processNo-' + str(os.getpid()))
            # print(process.memory_info().rss)
            print("Ended the process %s in %s " % (str(processNo), time.time() - seconds))
            process.terminate()
        except Exception as ex:
            print(ex)
            raise
