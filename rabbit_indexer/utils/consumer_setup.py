# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '30 Apr 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import argparse
import logging
from .yaml_config import YamlConfig
from pydoc import locate

logger = logging.getLogger(__name__)


def consumer_setup(consumer=None, description='Begin the rabbit based deposit indexer'):
    # Command line arguments to get rabbit config file.
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--config', dest='config', help='Path to config file for rabbit connection', required=True)

    args = parser.parse_args()

    CONFIG_FILE = args.config
    conf = YamlConfig()
    conf.read(CONFIG_FILE)

    # Setup logging
    logging_level = conf.get('logging', 'log-level')
    logger.setLevel(getattr(logging, logging_level.upper()))

    # Add formatting
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, logging_level.upper()))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    # Load the consumer
    if not consumer:
        consumer_class = conf.get('rabbit_server','queue_consumer_class')
        consumer = locate(consumer_class)

    logger.info(f'Loaded {consumer_class}')

    consumer = consumer(conf)
    consumer.run()