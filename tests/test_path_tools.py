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
import json

from rabbit_indexer.utils import PathTools


def get_local_path():
    return os.path.dirname(os.path.relpath(__file__))


class TestPathTools(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        mapping_file = os.path.join(get_local_path(), 'moles_mapping_file.json')
        cls.path_tools = PathTools(mapping_file=mapping_file)
        cls.directory_path = os.path.join(get_local_path(), 'test_tree/badc/cmip5')

        with open(mapping_file) as reader:
            mapping = json.load(reader)
            cls.moles_cmip5_metadata = list(mapping.values())[0]

    def test_generate_path_metadata(self):

        metadata, islink = self.path_tools.generate_path_metadata(
            'test_tree/badc/cmip5/data'
        )

        # Check have extracted the right metadata
        self.assertDictEqual(
            metadata,
            {
                'depth': 3,
                'dir': 'data',
                'path': 'test_tree/badc/cmip5/data',
                'archive_path': 'test_tree/badc/cmip5/data',
                'link': False,
                'type': 'dir',
                **self.moles_cmip5_metadata
            }
        )

        self.assertFalse(islink)

    def test_get_moles_record_metadata(self):
        metadata = self.path_tools.get_moles_record_metadata(
            'test_tree/badc/cmip5/data'
        )
        self.assertDictEqual(
            metadata,
            self.moles_cmip5_metadata
        )

    def test_get_readme(self):
        """
        Check can extract readme from directory path
        """
        readme = self.path_tools.get_readme(self.directory_path)
        self.assertEqual(readme, '5th Coupled Model Intercomparison Project (CMIP5)\n')

    def test_generate_id(self):
        """
        Check that id is a sha1 hash of the file path
        :return:
        """

        hash = self.path_tools.generate_id('test_tree/badc/cmip5')
        self.assertEqual('5174fa172be7d29d15fb0a2a09e7d600375585d9', hash)


if __name__ == '__main__':
    unittest.main()
