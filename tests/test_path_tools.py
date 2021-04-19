# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '08 Feb 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from pyfakefs.fake_filesystem_unittest import TestCase
import unittest
import os
import json

from rabbit_indexer.utils import PathTools, PathFilter


def get_local_path():
    return os.path.dirname(os.path.relpath(__file__))


class PathToolsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        mapping_file = os.path.join(get_local_path(), 'moles_mapping_file.json')
        cls.path_tools = PathTools(mapping_file=mapping_file)

        with open(mapping_file) as reader:
            mapping = json.load(reader)
            cls.moles_cmip5_metadata = list(mapping.values())[0]

    def setUp(self):
        """
        Set up fake directory structure
        :return:
        """
        self.setUpPyfakefs()

        dirs = [
            '/badc/cmip5/data',
            '/neodc/avhrr-3'
        ]
        files = [('/badc/cmip5/00README', '5th Coupled Model Intercomparison Project (CMIP5)\n', 'utf-8'),
                 ('/neodc/avhrr-3/00README', '(51.1445°N, 1.4370°W)', 'iso-8859-1'),
                 ]

        for _dir in dirs:
            self.fs.create_dir(_dir)

        for _file, content, encoding in files:
            self.fs.create_file(_file, contents=content, encoding=encoding)

    def test_tree_match(self):

        paths = [
            ('/neodc/avhrr-3', False),
            ('/badc/cmip5/data', True)
        ]

        for path, expected in paths:
            match = self.path_tools.tree.search_name(path)
            self.assertEqual(bool(match), expected)

    def test_generate_path_metadata(self):

        metadata, islink = self.path_tools.generate_path_metadata(
            '/badc/cmip5/data'
        )

        # Check have extracted the right metadata
        self.assertDictEqual(
            metadata,
            {
                'depth': 3,
                'dir': 'data',
                'path': '/badc/cmip5/data',
                'archive_path': '/badc/cmip5/data',
                'link': False,
                'type': 'dir',
                **self.moles_cmip5_metadata
            }
        )
        #
        self.assertFalse(islink)

        # Check that we still return collections
        metadata, islink = self.path_tools.generate_path_metadata('/neodc/avhrr-3')
        self.assertTrue(metadata.get('record_type'), 'Dataset Collection')

    def test_get_moles_record_metadata(self):
        metadata = self.path_tools.get_moles_record_metadata(
            '/badc/cmip5/data'
        )
        self.assertDictEqual(
            metadata,
            self.moles_cmip5_metadata
        )

    def test_get_moles_record_metadata_live(self):
        """
        Check against the live API

        :return:
        """

        paths = [
            '/neodc/avhrr-3',
        ]

        for path in paths:
            metadata = self.path_tools.get_moles_record_metadata(path)
            expected = self.path_tools._get_moles_record_metadata_data_from_api(path)
            self.assertDictEqual(metadata, expected)

    def test_get_readme(self):
        """
        Check can extract readme from directory path
        """
        readme = self.path_tools.get_readme('/badc/cmip5')
        self.assertEqual(readme, '5th Coupled Model Intercomparison Project (CMIP5)\n')

        readme = self.path_tools.get_readme('/neodc/avhrr-3')
        self.assertEqual(readme, '(51.1445�N, 1.4370�W)')

    def test_generate_id(self):
        """
        Check that id is a sha1 hash of the file path
        :return:
        """

        hash = self.path_tools.generate_id('test_tree/badc/cmip5')
        self.assertEqual('5174fa172be7d29d15fb0a2a09e7d600375585d9', hash)


class PathFilterTestCase(TestCase):

    def test_allow_deny_filter(self):
        filter_paths = ['/neodc/esacci']
        filter = PathFilter(paths=filter_paths, filter_policy=PathFilter.ALLOW_FILTER_DENY)

        test_paths = [
            ('/neodc/sentinel1b/data/TC_Sentinel_Data_31072014.pdf', True),
            ('/neodc/esacci/biomass/data/agb/maps/v2.0/00README_catalogue_and_licence.txt', False)
        ]

        for test_path, expected in test_paths:
            match = filter.allow_path(test_path)

            self.assertEqual(match, expected)

    def test_allow_all(self):
        filter = PathFilter(filter_policy=PathFilter.ALLOW_FILTER_DENY)

        test_paths = [
            ('/neodc/sentinel1b/data/TC_Sentinel_Data_31072014.pdf', True),
            ('/neodc/esacci/biomass/data/agb/maps/v2.0/00README_catalogue_and_licence.txt', True)
        ]

        for test_path, expected in test_paths:
            match = filter.allow_path(test_path)

            self.assertEqual(match, expected)


    def test_error_raised_on_invalid_mode(self):

        with self.assertRaises(ValueError):
            PathFilter(filter_policy=3)

    def test_deny_allow_filter(self):
        filter_paths = ['/neodc/esacci']
        filter = PathFilter(paths=filter_paths, filter_policy=PathFilter.DENY_FILTER_ALLOW)

        test_paths = [
            ('/neodc/sentinel1b/data/TC_Sentinel_Data_31072014.pdf', False),
            ('/neodc/esacci/biomass/data/agb/maps/v2.0/00README_catalogue_and_licence.txt', True)
        ]

        for test_path, expected in test_paths:
            match = filter.allow_path(test_path)

            self.assertEqual(match, expected)

    def test_deny_all(self):
        filter = PathFilter(filter_policy=PathFilter.DENY_FILTER_ALLOW)

        test_paths = [
            ('/neodc/sentinel1b/data/TC_Sentinel_Data_31072014.pdf', False),
            ('/neodc/esacci/biomass/data/agb/maps/v2.0/00README_catalogue_and_licence.txt', False)
        ]

        for test_path, expected in test_paths:
            match = filter.allow_path(test_path)

            self.assertEqual(match, expected)






if __name__ == '__main__':
    unittest.main()
