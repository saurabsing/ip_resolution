# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh
"""Module that provides service configuration."""

import os
import json


class ServiceConfig:
    """Provide service configuration."""

    def __init__(self):
        """Initialize instance of ServiceConfig."""
        self.config = {}

        current_dir = os.getcwd()
        env_name = 'local'
        env_variable_for_name = 'SP_ENVIRONMENT'
        if env_variable_for_name in os.environ:
            env_name = os.environ[env_variable_for_name]

        with open(os.path.join(current_dir, 'config', env_name + '.json')) as env_file:
            env_json = json.load(env_file)
            self.config = env_json.copy()

    def getConfig(self):
        """Return configuration contained within this instance.  Configuration is repesented in dict."""
        return self.config


def main():
    """Create instance of ServiceConfig."""
    config = ServiceConfig().getConfig()
    print(config['stream']['kafkaHost'])


if __name__ == "__main__":
    main()
