from handler.naive import AverageWorker

import os

import pickle

import logging

LOGLEVEL = os.environ.get('PYTHON_LOGLEVEL', 'INFO').upper()

logger = logging.getLogger(__name__)

logging.basicConfig(level=LOGLEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def restore_redis_from_postgres():
    """Restore history of the contact in campaign to Redis from the values saved in Postgres"""
    # this is useful in cases Redis failures
    AverageWorker.connect_redis_dialer()
    with AverageWorker.get_dialer_connection() as conn_dialer:
        cursor_dialer = conn_dialer.cursor()
        size = 1000
        cursor_dialer.execute('select history, id_contact, id_campaign, status, phone_numbers_list,'
                              ' phone_number_index from only contact_in_campaign;')
        while True:
            contacts = cursor_dialer.fetchmany(size=size)
            if not contacts:
                break
            logger.debug('Importing {0} contacts from Postgres to Redis'.format(len(contacts)))
            for (history, id_contact, id_campaign, status, phone_numbers,
                 phone_number_index) in contacts:
                for status_item in history:
                    AverageWorker.REDIS_DIALER_CONNECTION.rpush(
                        f'CONTACT:{id_contact}:CAMP:{id_campaign}:HISTORY', status_item)
                AverageWorker.REDIS_DIALER_CONNECTION.hset(
                    f'CONTACT:{id_contact}:CAMP:{id_campaign}', 'STATUS', status)
                if phone_number_index is not None:
                    AverageWorker.REDIS_DIALER_CONNECTION.hset(
                        f'CONTACT:{id_contact}:CAMP:{id_campaign}', 'PHONE_NUMBER_LIST',
                        pickle.dumps(phone_numbers))
                    AverageWorker.REDIS_DIALER_CONNECTION.hset(
                        f'CONTACT:{id_contact}:CAMP:{id_campaign}', 'PHONE_NUMBER_INDEX',
                        phone_number_index)
        cursor_dialer.execute('select id, statistics from only campaign;')
        for id_campaign, statistics in cursor_dialer.fetchall():
            for report, counter in statistics.items():
                AverageWorker.REDIS_DIALER_CONNECTION.hset(
                    f'CAMP:{id_campaign}:COUNTER', report, counter)


if __name__ == '__main__':
    restore_redis_from_postgres()
