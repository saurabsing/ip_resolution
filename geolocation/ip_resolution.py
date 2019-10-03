# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh

"""Module that provides core class which resolves IP adrress to location.

This module provides methods to process data frames data, hit Geolocation
service API, parses returned json object and creates final dataframe to
write to intermediate layer.
"""


import time
from job_config import JobConfig
import os
import psutil
import utility_functions as utility
from datetime import datetime


class IPResolver:
    """Provides implementation of parsing location from ip address and dataframes."""

    def __init__(self):
        """Initialize instance of IP Resolver."""
        self.config = JobConfig().getconfig()
        self.logger = utility.getlogger('ip_resolution', 'ip_resolution')

    def setlogger(self, processNo):
        """Get Logger for use in a python child process."""
        self.logger = utility.getlogger('ip_resolution', 'ipresolver', processNo)

    def preprocess(self, dataframe_ip_address):
        """Do any preprocessing on data."""
        try:
            df = dataframe_ip_address
            df['ipaddress_stripped'] = df['ipaddress'].apply(lambda x: x.strip('::ffff:'))
            df.assign(city='', country='', region='', createdAt='')
            return df
        except Exception as ex:
            self.logger.info('Issue in preprocessing logic:' + ex)
            self.logger.error(utility.print_exception())

    def postprocess(self, dataframe_ip_address):
        """Do any postprocessing on data after ip resolution."""
        try:
            df = dataframe_ip_address
            df['country'] = df['country'].apply(lambda x: str(x).replace("'", "") if x else x)
            df['city'] = df['city'].apply(lambda x: str(x).replace("'", "") if x else x)
            df['region'] = df['region'].apply(lambda x: str(x).replace("'", "") if x else x)
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            df['createdAt'] = dt_string
            return df
        except Exception as ex:
            self.logger.info('Issue in postprocessing logic:' + ex)
            self.logger.error(utility.print_exception())

    def parse_json_address(self, addressObject):
        """Parse address json object."""
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
        except Exception as ex:
            raise ex
        return country, region, city

    def get_address(self, dataframe_ip_address, geocoding_service):
        """Call Geocoding service API."""
        try:
            country, region, city = '', '', ''
            df = dataframe_ip_address
            for ind, row_series in df.iterrows():
                try:
                    ipaddress = df['ipaddress_stripped'][ind]
                    addressObject = geocoding_service.fetch_geolocation(ipaddress)
                    country, region, city = self.parse_json_address(addressObject)
                    df.at[ind, 'country'] = country
                    df.at[ind, 'region'] = region
                    df.at[ind, 'city'] = city
                    # print(addressObject)
                except Exception as ex:
                    self.logger.info('Issue with fetching or parsing logic:' + ex)
                    self.logger.error(utility.print_exception())
            return df
        except Exception as ex:
            self.logger.info('Issue with getIpInformation logic:' + ex)
            self.logger.error(utility.print_exception())

    def resolve_ipaddress(self, dataframe_ip_address, geocoding_service, datawriter, processNo):
        """Run the ip resolution methods in a child process."""
        try:

            seconds = time.time()
            self.setlogger(processNo)
            geocoding_service.setlogger(processNo)
            self.logger.info("Started the process %s at %s " % (str(processNo), time.time()))
            dataframe_ip_preprocessed = self.preprocess(dataframe_ip_address)
            dataframe_ip_processed = self.get_address(dataframe_ip_preprocessed, geocoding_service)
            dataframe_results = self.postprocess(dataframe_ip_processed)
            # print(dataframe_results)
            datawriter.setlogger(processNo)
            datawriter.append_data(dataframe_results, processNo)
            process = psutil.Process(os.getpid())
            # print('processNo-' + str(os.getpid()))
            # print(process.memory_info().rss)
            self.logger.info("Ended the process %s in %s " % (str(processNo), time.time() - seconds))
            process.terminate()
        except Exception as ex:
            self.logger.info('Issue with resolveIp logic in IpResolver:' + ex)
            self.logger.error(utility.print_exception())
