#!/bin/bash
# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh


nohup python3 /home/ubuntu/ipAddress/geolocation/resolve_ip.py &> /home/ubuntu/logs/run.log &
exit $?
