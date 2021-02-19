# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '17 Feb 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import yaml


class YamlConfig:

    def __init__(self):
        self.config = None

    def read(self, filepath):
        with open(filepath) as reader:
            self.config = yaml.load(reader, Loader=yaml.FullLoader)

    def get(self, *args, default=None):
        """
        Takes an iterable of keys and returns default if not found or the value
        :param args: key strings to serch config
        :param default: The default response object if the key not found
        :return:
        """

        if not self.config:
            raise ValueError('No config loaded')

        last_key = args[-1]
        dict_nest = self.config

        for key in args:
            if key != last_key:
                dict_nest = dict_nest.get(key, {})
            else:
                return dict_nest.get(key, default)
