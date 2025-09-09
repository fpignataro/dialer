# -*- coding: utf-8 -*-

# only for testing by hand

from handler.naive import AverageWorker

import logging

import os

LOGLEVEL = os.environ.get('PYTHON_LOGLEVEL', 'INFO').upper()

logger = logging.getLogger(__name__)

logging.basicConfig(level=LOGLEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def clean():
    """Clean Redis DB and contact_in_campaign table in Postgres for be able to test again the main
    process of a campaign from scratch"""
    AverageWorker.connect_redis_dialer()
    AverageWorker.REDIS_DIALER_CONNECTION.flushdb()

    # setting all contacts for campaign 4 in status == CREATED
    with AverageWorker.get_dialer_connection() as conn_dialer:
        cursor_dialer = conn_dialer.cursor()
        cursor_dialer.execute(
            "UPDATE contact_in_campaign SET status = 2, history = '{}' "
            "WHERE id_campaign = 4;"
        )

    logger.debug('Campaign is ready to be started from scratch again')


if __name__ == '__main__':
    clean()
