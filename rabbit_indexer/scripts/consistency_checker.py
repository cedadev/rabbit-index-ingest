# encoding: utf-8
"""
Reads directories off a queue and checks elasticsearch to make sure that the filesystem and index are in harmony
"""
__author__ = 'Richard Smith'
__date__ = '04 Jun 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

# TODO: Make sure can handler where files are on tape

import persistqueue
from configparser import RawConfigParser
import os
import time
from rabbit_indexer.utils import get_line_in_file
import requests
import logging


class ElasticsearchConsistencyChecker:

    def __init__(self):
        base = os.path.dirname(__file__)
        self.default_config = os.path.join(base, '../conf/index_updater.ini')
        self.default_db_path = os.path.join(base, '../data')

        self.conf = RawConfigParser()
        self.conf.read(self.default_config)

        # Load queue params to object
        self._load_queue_params()

        # Setup queues
        self.manual_queue = persistqueue.SQLiteAckQueue(
            os.path.join(self.db_location, 'priority'),
            multithreading=True
        )
        self.bot_queue = persistqueue.SQLiteAckQueue(
            os.path.join(self.db_location, 'bot'),
            multithreading=True
        )

        self.spot_progress = self._get_spot_progress()

    def _load_queue_params(self):

        self.local_queue = self.conf['local-queue']

        self.db_location = self.local_queue.get('queue-location', self.default_db_path)
        self.spot_file = os.path.join(self.db_location, 'spot_file.txt')
        self.progress_file = os.path.join(self.db_location, 'spot_progress.txt')


    def _get_spot_progress(self):
        """
        Set the line to read from the spot file on initialisation

        :return: int
        """


        if os.path.exists(self.progress_file):
            with open(self.progress_file) as reader:
                line = reader.readline()

            if line:
                return int(line.strip())

        return 0

    def _update_spot_progress(self):
        """
        Write the progress to file so that it persists if the main process dies
        """
        self.spot_progress += 1
        logging.debug(f'Spot progress: {self.spot_progress}')

        with open(self.progress_file, 'w') as writer:
            writer.write(str(self.spot_progress))

    def _download_spot_conf(self):
        """
        Download spot configuration file and write to disk
        """

        url = self.local_queue.get('spot-url')

        r = requests.get(url)

        with open(self.spot_file, 'w') as writer:
            writer.write(r.text)

        self.spot_progress = 0

    def process_queue(self, queue):
        """
        Perform action on the queue and acknowledge when done

        :param queue: queue name
        """

        q = getattr(self, queue)

        item = q.get()
        print(item)
        time.sleep(3)
        q.ack(item)

    def get_next_spot(self):
        """
        Get the next spot to add to the bot queue

        :return: Next spot
        """

        # Download the configuration if it does not exist
        if not os.path.exists(self.spot_file):
            logging.debug('Spot file does not exist. Downloading...')
            self._download_spot_conf()

        # Increment spot_progress to retrieve next line
        self._update_spot_progress()

        # Get the line
        line = get_line_in_file(self.spot_file, self.spot_progress)

        if line:
            spot, path = line.strip().split()
            logging.debug(f'Loading spot: {path}')

        else:
            # Reached EOF. Download new file
            logging.debug('Reached end of spot file. Downloading new spot file')
            self._download_spot_conf()
            self._update_spot_progress()

            # Get first line
            line = get_line_in_file(self.spot_file, self.spot_progress)
            spot, path = line.strip().split()
            logging.debug(f'Loading spot: {path}')

        return path

    def add_dirs_to_queue(self, path):
        """
        Walks a directory tree, given a path and adds the directories to the bot queue
        """

        for root, dirs, _ in os.walk(path):
            abs_root = os.path.abspath(root)

            for dir in dirs:
                self.bot_queue.put(os.path.join(abs_root, dir))

    def start_consuming(self):

        manual_qsize = self.manual_queue._count()
        bot_qsize = self.bot_queue._count()

        if manual_qsize:
            self.process_queue('manual_queue')

        elif bot_qsize:
            self.process_queue('bot_queue')

        if bot_qsize == 0:
            spot = self.get_next_spot()
            self.add_dirs_to_queue(spot)

    @classmethod
    def main(cls):

        checker = cls()

        while True:
            try:
                checker.start_consuming()

            except KeyboardInterrupt:
                break


if __name__ == '__main__':
    ElasticsearchConsistencyChecker.main()
