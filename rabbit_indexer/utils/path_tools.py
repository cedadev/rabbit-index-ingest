"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Jan 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.core.log_reader import SpotMapping
import os
import requests
from json.decoder import JSONDecodeError
import hashlib
from requests.exceptions import Timeout


class PathTools:

    def __init__(self, moles_mapping_url='http://api.catalogue.ceda.ac.uk/api/v0/obs/all'):

        self.moles_mapping_url = moles_mapping_url

        self.spots = SpotMapping()

        try:
            self.moles_mapping = requests.get(moles_mapping_url).json()
        except JSONDecodeError as e:
            import sys
            raise ConnectionError(f'Could not connect to {moles_mapping_url} to get moles mapping') from e


    def generate_path_metadata(self, path):
        """
        Take path and process it to generate metadata as used in ceda directories index
        :param path:
        :return:
        """
        if not os.path.isdir(path):
            return None, None

        # See if the path is a symlink
        link = os.path.islink(path)

        # Set the archive path
        archive_path = path

        # If the path is a link, we need to find the path to the actual data
        if link:
            archive_path = self.spots.get_archive_path(path)

        # Create the metadata
        dir_meta = {
            'depth': path.count('/'),
            'dir': os.path.basename(path),
            'path': path,
            'archive_path': archive_path,
            'link': link,
            'type': 'dir'
        }

        # Retrieve the appropriate MOLES record
        record = self.get_moles_record_metadata(path)

        # If a MOLES record is found, add the metadata
        if record and record['title']:
            dir_meta['title'] = record['title']
            dir_meta['url'] = record['url']
            dir_meta['record_type'] = record['record_type']

        return dir_meta, dir_meta['link']

    def get_moles_record_metadata(self, path):
        """
        Try and find metadata for a MOLES record associated with the path.
        :param path: Directory path
        :return: Dictionary containing MOLES title, url and record_type
        """

        # Condition path - remove trailing slash
        if path.endswith('/'):
            path = path[:-1]

        # Check for path match in stored dictionary
        test_path = path
        while test_path != '/' and test_path:

            result = self.moles_mapping.get(test_path)

            # Try adding a slash to see if it matches. Some records in MOLES are stored
            # with a slash, others are not
            if not result:
                result = self.moles_mapping.get(test_path + '/')

            if result is not None:
                return result

            # Shrink the path down until a match is found
            test_path = os.path.dirname(test_path)

        # No match has been found
        # Search MOLES API for path match
        url = f'http://api.catalogue.ceda.ac.uk/api/v0/obs/get_info{path}'
        try:
            response = requests.get(url, timeout=10)
        except Timeout:
            return

        # Update moles mapping
        if response:
            self.moles_mapping[path] = response.json()
            return response.json()

    @staticmethod
    def get_readme(path):
        """
        Search in directory for a README file and read the contents
        :param path: Directory path
        :return: Readme contents
        """
        if '00README' in os.listdir(path):
            with open(os.path.join(path, '00README')) as reader:
                content = reader.read()

            return content.encode(errors='ignore').decode()

    def update_mapping(self):

        successful = True
        # Update the moles mapping
        try:
            self.moles_mapping = requests.get(self.moles_mapping_url, timeout=30).json()
        except (ValueError, Timeout):
            successful = False

        # Update the spot mapping
        self.spots = SpotMapping()

        return successful

    @classmethod
    def generate_id(cls, path):
        """
        Take a path, encode to utf-8 (ignoring non-utf8 chars) and return hash
        :param path:
        :return: hash
        """

        return hashlib.sha1(path.encode(errors='ignore')).hexdigest()