# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '24 Sep 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import os
from functools import wraps
from time import sleep

DELAY = 20

def wait_for_file(func):
    """
    Will check if the path exists. If it does not, it will
    initiate a sleep for 'delay' seconds
    :param func: function to wrap
    :return: wrapped function
    """
    @wraps(func)
    def wrapper(self, path):
        if not os.path.exists(path):
            sleep(DELAY)
        return func(self, path)
    return wrapper


class Test():
    day = 'tuesday'

    @wait_for_file
    def filename(self, path):
        print(self.day)
        print(path)



if __name__ == '__main__':
    t = Test()

    t.filename(path='path_tools.pys')
