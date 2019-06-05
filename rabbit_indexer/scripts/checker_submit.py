# encoding: utf-8
"""
Command line utility to submit directories to the consistency checker to compare the filesystem to elasticsearch indices

"""
__author__ = 'Richard Smith'
__date__ = '31 May 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


import argparse
import os
from rabbit_indexer.utils import query_yes_no
from configparser import RawConfigParser
import persistqueue

class DirectorySubmitter:

    def __init__(self, args):

        self.conf = RawConfigParser()
        self.conf.read(args.conf)
        self.queue = persistqueue.SQLiteAckQueue(os.path.join(self.conf.get('local-queue','queue-location'), 'priority'), multithreading=True)

    def get_directories(self, args):

        directories = []

        if self.check_path(args.dir):

            if args.recursive:
                for root, dirs, _ in os.walk(args.dir):
                    abs_root = os.path.abspath(root)

                    for dir in dirs:
                        directories.append(os.path.join(abs_root, dir))

            else:
                listing = os.listdir(args.dir)
                abs_root = os.path.abspath(args.dir)

                for _dir in listing:
                    path = os.path.join(abs_root, _dir)

                    if os.path.isdir(path):
                        directories.append(path)

            return directories

        raise NotADirectoryError(f'{args.dir} is not a directory')

    @staticmethod
    def check_path(path):
        """
        Check that we have been given a real directory
        :param path:
        :return: boolean
        """
        return bool(os.path.exists(path) and os.path.isdir(path))


    def process_directories(self, args):

        directories = self.get_directories(args)

        if query_yes_no(f'Found {len(directories)} directories. Continue?'):
            for _dir in directories:
                self.queue.put(_dir)


    @staticmethod
    def get_args():
        """
        Command line arguments
        :return:
        """

        default_config = os.path.join(os.path.dirname(__file__), '../conf/index_updater.ini')

        parser = argparse.ArgumentParser(description='Submit directories to be checked for consistency'
                                                     ' with the elasticsearch indices containing files'
                                                     ' and directories')

        parser.add_argument('dir', type=str, help='Path to submit to consistency checker')
        parser.add_argument('-r', dest='recursive', action='store_true',
                            help='Recursive. Will include all directories below this point as well')
        parser.add_argument('--conf', type=str, default=default_config, help='Optional path to configuration file')

        return parser.parse_args()

    @classmethod
    def main(cls):

        args = cls.get_args()

        checker = cls(args)

        checker.process_directories(args)

def main():
    DirectorySubmitter.main()

if __name__ == '__main__':
    main()