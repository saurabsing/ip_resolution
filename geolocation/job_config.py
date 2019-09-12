# Copyright (c) 2017 Square Panda Inc.
# All Rights Reserved.
# Dissemination, use, or reproduction of this material is strictly forbidden
# unless prior written permission is obtained from Square Panda Inc.
# @Last modified by:   Singh Saurabh
"""Module that provides job configuration."""

import os
import json


class JobConfig:
    """Provide job configuration."""

    def __init__(self):
        """Initialize instance of JobConfig."""
        self.config = {}

        current_dir = os.getcwd()
        env_name = 'local'
        env_variable_for_name = 'SP_ENVIRONMENT'
        if env_variable_for_name in os.environ:
            env_name = os.environ[env_variable_for_name]

        with open(os.path.join(current_dir, 'config', env_name + '.json')) as env_file:
            env_json = json.load(env_file)
            self.config = env_json.copy()

    def getconfig(self):
        """Return configuration contained within this instance.Configuration is repesented in dict."""
        return self.config


def main():
    """Create instance of JobConfig."""
    config = JobConfig().getConfig()
    print(config['test']['test'])


if __name__ == "__main__":
    main()
