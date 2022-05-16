"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Jan 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from pathlib import Path
from ceda_elasticsearch_tools.core.log_reader import SpotMapping
import os
import requests
from json.decoder import JSONDecodeError
import json
import hashlib
from requests.exceptions import Timeout
from directory_tree import DatasetNode

from typing import Optional, Tuple, List


def process_observations(results):
    """
    Convert the result list into a mapping object
    :param results: list of observation json objects
    :return: object map
    """

    processed_map = {}
    for result in results:

        # Skip results where the publication state is working
        if result.get('publicationState') == 'working':
            continue

        # Skip where the result_field value is None
        if result['result_field'] is None:
            continue

        data_path = result['result_field']['dataPath'].rstrip('/')

        try:
            processed_map[data_path] = {
                'title': result['title'],
                'url': f'https://catalogue.ceda.ac.uk/uuid/{result["uuid"]}',
                'record_type': 'Dataset'
            }
        except TypeError:
            continue

    return processed_map


def generate_moles_mapping(api_url, mapping=None):
    """
    Use the MOLES v2 API to generate a mapping from dataset path to moles record

    :param api_url: MOLES api URL
    :param mapping: Used for recursive functionality
    :return: Mapping dict
    """

    # Set the dictionary on first calling
    if not mapping:
        mapping = {}

    # Get the api response
    try:
        response = requests.get(api_url).json()
    except JSONDecodeError as e:
        import sys
        raise ConnectionError(f'Could not connect to {api_url} to get moles mapping') from e

    # Turn response into mapping object
    mapping.update(process_observations(response['results']))

    if not response['next']:
        return mapping
    else:
        return generate_moles_mapping(response['next'], mapping)


def load_moles_mapping(mapping_file):
    """
    Load a json document and condition it to match same as from API
    """

    data = {}

    with open(mapping_file) as reader:
        json_doc = json.load(reader)

    # Loop through and remove trailing slash from paths
    for k, v in json_doc.items():
        data[k.rstrip('/')] = v

    return data


class PathTools:

    def __init__(self,
                 moles_mapping_url: str = 'http://api.catalogue.ceda.ac.uk/api/v2/observations.json/',
                 mapping_file: Optional[str] = None):

        self.moles_mapping_url = moles_mapping_url

        self.spots = SpotMapping()

        if mapping_file:
            self.moles_mapping = load_moles_mapping(mapping_file)
        else:
            self.moles_mapping = generate_moles_mapping(self.moles_mapping_url)

        # Setup the matching tree
        self.tree = DatasetNode()
        for path in self.moles_mapping:
            self.tree.add_child(path)

    def generate_path_metadata(self, path: str) -> Tuple[Optional[dict], Optional[bool]]:
        """
        Take path and process it to generate metadata as used in ceda directories index
        :param path: path to retrieve metadata for
        :return:
        """

        path = Path(path)

        if not path.exists():
            return None, None

        # See if the path is a symlink and directory
        link = path.is_symlink()
        isdir = path.is_dir()

        # Set the archive path
        archive_path = path

        # If the path is a link, we need to find the path to the actual data
        if link:
            link_path = os.readlink(path)
            if not link_path.startswith(("/datacentre", "..")):
                archive_path = link_path
            elif link_path.startswith(".."):
                count = link_path.count("../")
                link_path = link_path.lstrip("./")
                archive_path = path.parents[count] / link_path


        # Create the metadata
        meta = {
            'depth': len(path.parts) - 1,
            'dir': path.name,
            'path': str(path),
            'archive_path': str(archive_path),
            'link': link,
            'type': 'dir' if isdir else 'file'
        }

        # Retrieve the appropriate MOLES record
        if isdir:
            record = self.get_moles_record_metadata(path)

            # If a MOLES record is found, add the metadata
            if record and record['title']:
                meta['title'] = record['title']
                meta['url'] = record['url']
                meta['record_type'] = record['record_type']

        return meta, meta['link']

    def get_moles_record_metadata(self, path: str) -> Optional[dict]:
        """
        Try and find metadata for a MOLES record associated with the path.

        :param path: Directory path
        :return: Dictionary containing MOLES title, url and record_type
        """

        # Condition path - remove trailing slash
        path = path.rstrip('/')

        # Search the tree
        match = self.tree.search_name(path)
        if match:
            result = self.moles_mapping.get(path)

            if result:
                return result

        return self._get_moles_record_metadata_data_from_api(path)

    def _get_moles_record_metadata_data_from_api(self, path: str) -> Optional[dict]:
        """
        Request metadata from the API, this is used as a last resort

        :param path: Path to retrieve metadata for
        :return: Metadata dict | None
        """

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
    def get_readme(path: str) -> Optional[str]:
        """
        Search in directory for a README file and read the contents
        :param path: Directory path
        :return: Readme contents
        """
        if not os.path.exists(path):
            return

        if '00README' in os.listdir(path):
            with open(os.path.join(path, '00README'), errors='replace') as reader:
                content = reader.read()

            return content.encode(errors='ignore').decode()

    def update_mapping(self) -> bool:

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
    def generate_id(cls, path: str) -> str:
        """
        Take a path, encode to utf-8 (ignoring non-utf8 chars) and return hash

        :param path: filepath to hash
        :return: hash hexdigest of sha1 hash
        """

        return hashlib.sha1(path.encode(errors='ignore')).hexdigest()


class PathFilter:
    """
    Allows filtering of messages based on filepath. There are two
    filtering rules:
    1. ALLOW_FILTER_DENY (default) - Allow all paths, deny those listed in the filter
    2. DENY_FILTER_ALLOW - Deny all paths, allow those listed by the filter
    """

    ALLOW_FILTER_DENY = 1
    DENY_FILTER_ALLOW = 2
    FILTER_POLICY_VALUE_LIST = [
        ALLOW_FILTER_DENY,
        DENY_FILTER_ALLOW
    ]

    def __init__(self, paths: Optional[List[str]] = None, filter_policy: int = 1):
        """
        :param paths: List of paths to pass to the filter
        :param: filter_policy: Rule to apply with the filter.
                            Defaults to value of ALLOW_FILTER_DENY i.e. Allow all
                            Can use class attributes to make it easy to read. e.g.

                            filter = PathFilter(filter_type=PathFilter.ALLOW_FILTER_DENY)
        """

        if filter_policy not in self.FILTER_POLICY_VALUE_LIST:
            string_value_list = [str(x) for x in self.FILTER_POLICY_VALUE_LIST]
            raise ValueError(f'filter_policy must be an integer with value in [{",".join(string_value_list)}].'
                             f'You have provided {filter_policy}')

        self.filter_mode = filter_policy
        self.tree = DatasetNode()

        if paths:
            for path in paths:
                self.tree.add_child(path)

    def allow_path(self, path: str) -> bool:
        """
        :param path: The path to test.

        :return: bool
        """

        match = self.tree.search_name(path)

        # Deny matches
        if self.filter_mode == self.ALLOW_FILTER_DENY:
            return not bool(match)

        # Allow matches
        elif self.filter_mode == self.DENY_FILTER_ALLOW:
            return bool(match)

        raise ValueError(f'Selected filter mode {self.filter_mode}, does not have a policy')
