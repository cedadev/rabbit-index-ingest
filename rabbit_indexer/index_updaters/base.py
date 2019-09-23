
from datetime import datetime
import logging
import json
from collections import namedtuple

IngestMessage = namedtuple('IngestMessage',['datetime','filepath','action','filesize','message'])

class UpdateHandler:

    def __init__(self, path_tools, conf, refresh_interval=30):

        # Initialise update counter
        self.update_time = datetime.now()
        self.refresh_interval = refresh_interval * 60 # convert to seconds

        # Initialise Path Tools
        self.pt = path_tools

        # Setup logging
        self.logger = logging.getLogger()
        logging_level = conf.get('logging', 'log-level')
        self.logger.setLevel(getattr(logging, logging_level.upper()))

    def _update_mappings(self):
        """
        Need to make sure that the code is using the most up to date mapping, either in
        MOLES or the spot mapping.
        :return:
        """

        timedelta = (datetime.now() - self.update_time).seconds

        # Refresh if ∆t is greater than the refresh interval and another thread has not already
        # picked up the task.
        if timedelta > self.refresh_interval:
            self.logger.info('Refreshing mappings')

            successful = self.pt.update_mapping()

            # Reset timer if mapping update successful
            if successful:
                self.update_time = datetime.now()

    @staticmethod
    def _decode_message(body):
        """
        Takes the message and turns into a dictionary.
        String message format when split on :
            date_hour = split_line[0]
            min = split_line[1]
            sec = split_line[2]
            path = split_line[3]
            action = split_line[4]
            filesize = split_line[5]
            message = ":".join(split_line[6:])

        :param body: Message body, either a json string or text
        :return: dictionary of format
            {
                'datetime': ':'.join(split_line[:3]),
                'filepath': split_line[3],
                'action': split_line[4],
                'filesize': split_line[5],
                'message': ':'.join(split_line[6:])
            }

        """


        try:
            msg = json.loads(body)
            return IngestMessage(**msg)

        except json.JSONDecodeError:
            # Assume the message is in the old format and split on :
            split_line = body.strip().split(":")

            msg = {
                'datetime': ':'.join(split_line[:3]),
                'filepath': split_line[3],
                'action': split_line[4],
                'filesize': split_line[5],
                'message': ':'.join(split_line[6:])
            }

        return IngestMessage(**msg)
