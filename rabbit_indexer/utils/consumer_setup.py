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

    parser.add_argument(
        '--config',
        dest='config',
        help='Path to config file for rabbit connection',
        nargs='+',
        required=True
    )

    args = parser.parse_args()

    CONFIG_FILE = args.config
    conf = YamlConfig()
    conf.read(CONFIG_FILE)

    # Setup logging
    log_level_str = conf.get('logging', 'log_level', default='info')
    log_level = getattr(logging, log_level_str.upper())

    logging.basicConfig(format='%(asctime)s @%(name)s [%(levelname)s]:    %(message)s', level=log_level)

    # Load the consumer
    if not consumer:
        consumer = conf.get('indexer', 'queue_consumer_class')
        consumer = locate(consumer)

    logger.info(f'Loaded {consumer}')

    consumer = consumer(conf)
    consumer.run()
