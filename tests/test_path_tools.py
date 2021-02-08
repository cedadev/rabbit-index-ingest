# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '08 Feb 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


from unittest import TestCase
import unittest
import os

from rabbit_indexer.utils import PathTools


def get_local_path():
    return os.path.dirname(os.path.abspath(__file__))


class TestPathTools(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        mapping_file = os.path.join(get_local_path(),'moles_mapping_file.json')
        cls.path_tools = PathTools(mapping_file=mapping_file)
        cls.local_path = get_local_path()

    def test_generate_path_metadata(self):
        pass

    def test_get_moles_record_metadata(self):
        pass

    def test_get_readme(self):
        """
        Check can extract readme from directory path
        """
        path = os.path.join(self.local_path, 'test_tree/badc/cmip5')
        readme = self.path_tools.get_readme(path)
        self.assertEqual(readme,'5th Coupled Model Intercomparison Project (CMIP5)\n')
    
    def test_update_mapping(self):
        pass

    def test_generate_id(self):
        pass


if __name__ == '__main__':
    unittest.main()