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
from pyfakefs.fake_filesystem_unittest import TestCase



def get_local_path():
    return os.path.dirname(os.path.relpath(__file__))

BASE_MESSAGE_CONTENTS = {
    'datetime': '2021-02-09 11:17:12',
    'action': 'MKDIR',
    'filesize': '',
    'message': ''
}

CONFIG_FILE = os.path.join(get_local_path(),'../rabbit_indexer/conf/index_updater_slow.ini.tmpl')
MAPPING_FILE = os.path.join(get_local_path(),'moles_mapping_file.json')


@patch('ceda_elasticsearch_tools.index_tools.base.IndexUpdaterBase._bulk_action')
class DirectoryUpdateTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        path_tools = PathTools(mapping_file=MAPPING_FILE)

        conf = RawConfigParser()
        conf.read(CONFIG_FILE)

        cls.handler = DirectoryUpdateHandler(path_tools, conf)

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
        files = [
            ('/neodc/avhrr-3/00README','test readme content'),
            ('/badc/cmip5/00README','5th Coupled Model Intercomparison Project (CMIP5)\\n"}\n')
        ]

        for _dir in dirs:
            self.fs.create_dir(_dir)

        for _file, content in files:
            self.fs.create_file(_file, contents=content)

    def test__process_creations(self, mock_method):

        expected = [
            unittest.mock.call(['{"update": {"_index": "ceda-dirs", "_id": "0ca121763e49eb46bfc209c455cb85773ecfcfa7"}}\n{"doc": {"depth": 3, "dir": "data", "path": "/badc/cmip5/data", "archive_path": "/badc/cmip5/data", "link": false, "type": "dir", "title": "WCRP CMIP5: Geophysical Fluid Dynamics Laboratory (GFDL) GFDL-CM3 model output for the historicalGHG experiment", "url": "http://catalogue.ceda.ac.uk/uuid/2e46c98da23c4c8da7942b3c4f8bee08", "record_type": "Dataset"}, "doc_as_upsert": true}\n']),
            unittest.mock.call(['{"update": {"_index": "ceda-dirs", "_id": "69bf498a75a2dfb32b17b424331428c549e6e9b5"}}\n{"doc": {"depth": 2, "dir": "cmip5", "path": "/badc/cmip5", "archive_path": "/badc/cmip5", "link": false, "type": "dir", "readme": "5th Coupled Model Intercomparison Project (CMIP5)\\\\n\\"}\\n"}, "doc_as_upsert": true}\n']),
            unittest.mock.call(['{"update": {"_index": "ceda-dirs", "_id": "f57978b502a15586e51364d63d9a711b01b2d3b6"}}\n{"doc": {"depth": 2, "dir": "avhrr-3", "path": "/neodc/avhrr-3", "archive_path": "/neodc/avhrr-3", "link": false, "type": "dir", "title": "AVHRR-3: Radiometric images", "url": "http://catalogue.ceda.ac.uk/uuid/7767578df074e685932237a00ef319c5", "record_type": "Dataset Collection", "readme": "test readme content"}, "doc_as_upsert": true}\n'])
        ]

        paths = [
            '/badc/cmip5/data',
            '/badc/cmip5',
            '/neodc/avhrr-3'
        ]

        for path in paths:
            message = IngestMessage(**{
                **BASE_MESSAGE_CONTENTS,
                'filepath': path
            })
            self.handler._process_creations(message)

        self.assertEqual(mock_method.mock_calls, expected)

    def test__process_deletions(self, mock_method):
        pass

    def test__process_symlinks(self, mock_method):
        pass

    def test__process_readmes(self, mock_method):
        pass


if __name__ == '__main__':
    unittest.main()
