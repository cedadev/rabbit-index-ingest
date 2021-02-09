# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '09 Feb 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import unittest
from rabbit_indexer.queue_handler.queue_handler import IngestMessage
from rabbit_indexer.utils import PathTools
from rabbit_indexer.index_updaters.directory_updates import DirectoryUpdateHandler
from configparser import RawConfigParser
import os
from unittest.mock import patch


def get_local_path():
    return os.path.dirname(os.path.relpath(__file__))


MESSAGE = IngestMessage(**{
    'datetime': '2021-02-09 11:17:12',
    'filepath': os.path.join(get_local_path(),'test_tree/badc/cmip5/data'),
    'action': 'MKDIR',
    'filesize': '',
    'message': ''
})
CONFIG_FILE = os.path.join(get_local_path(),'../rabbit_indexer/conf/index_updater_slow.ini.tmpl')
MAPPING_FILE = os.path.join(get_local_path(),'moles_mapping_file.json')


@patch('ceda_elasticsearch_tools.index_tools.base.IndexUpdaterBase._bulk_action')
class DirectoryUpdateTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.MESSAGE = MESSAGE
        path_tools = PathTools(mapping_file=MAPPING_FILE)

        conf = RawConfigParser()
        conf.read(CONFIG_FILE)

        cls.handler = DirectoryUpdateHandler(path_tools, conf)

    def test__process_creations(self, mock_method):

        expected = [
            unittest.mock.call(['{"index": {"_index": "ceda-dirs", "_id": "26b4449a0268a4da357c0993bd0eb98e6946f508"}}\n{"depth": 4, "dir": "data", "path": "tests/test_tree/badc/cmip5/data", "archive_path": "tests/test_tree/badc/cmip5/data", "link": false, "type": "dir"}\n']),
            unittest.mock.call(['{"update": {"_index": "ceda-dirs", "_id": "5036fbd8f8c96f185e42321aa506033e5fb7a609"}}\n{"doc": {"readme": "5th Coupled Model Intercomparison Project (CMIP5)\\n"}, "doc_as_upsert": true}\n']),
        ]

        self.handler._process_creations(self.MESSAGE)
        self.assertEqual(mock_method.mock_calls, expected)

    def test__process_deletions(self, mock_method):
        pass

    def test__process_symlinks(self, mock_method):
        pass

    def test__process_readmes(self, mock_method):
        pass


if __name__ == '__main__':
    unittest.main()
