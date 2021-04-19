# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '17 Feb 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import yaml
import os


class YamlMergeError(Exception):
    pass

class YamlConfig:

    def __init__(self):
        self.config = {}

    def read(self, filenames, encoding=None):
        """
        Read and parse a filename or an iterable of filenames.
        Files that cannot be opened are silently ignored; this is
        designed so that you can specify an iterable of potential
        configuration file locations (e.g. current directory, user's
        home directory, systemwide directory), and all existing
        configuration files in the iterable will be read.  A single
        filename may also be given.

        Return list of successfully read files.

        :param filenames: iterable of filenames
        :param encoding: file encoding for open command

        :return: List of successfully read files
        """
        if isinstance(filenames, (str, bytes, os.PathLike)):
            filenames = [filenames]
        read_ok = []

        for filename in filenames:
            try:
                with open(filename, encoding=encoding) as reader:
                    self.config = self.data_merge(
                        self.config,
                        yaml.load(reader, Loader=yaml.FullLoader)
                    )
            except OSError:
                continue

            if isinstance(filename, os.PathLike):
                filename = os.fspath(filename)

            read_ok.append(filename)

        return read_ok

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

    def data_merge(self, a, b):
        """
        Merges b into a and return merged result

        NOTE: tuples and arbitrary objects are not handled as it is totally ambiguous what should happen
        """
        key = None

        try:
            if a is None or isinstance(a, str) or isinstance(a, int) or isinstance(a, float):
                # border case for first run or if a is a primitive
                a = b
            elif isinstance(a, list):
                # lists can be only appended
                if isinstance(b, list):
                    # merge lists
                    a.extend(b)
                else:
                    # append to list
                    a.append(b)
            elif isinstance(a, dict):
                # dicts must be merged
                if isinstance(b, dict):
                    for key in b:
                        if key in a:
                            a[key] = self.data_merge(a[key], b[key])
                        else:
                            a[key] = b[key]
                else:
                    raise YamlMergeError(f'Cannot merge non-dict "{b}" into dict "{a}"')
            else:
                raise YamlMergeError(f'NOT IMPLEMENTED "{b}" into "{a}"')

        except TypeError as e:
            raise YamlMergeError(f'TypeError "{e}" in key "{key}" when merging "{b}" into "{a}"')

        return a
