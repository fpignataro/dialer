# -*- coding: utf-8 -*-

from flask import Flask, request

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from datetime import datetime

from settings.default import REDIS_DIALER_PORT, REDIS_DIALER_SERVER, GEARMAN_JOB_SERVERS

import gearman.client

import json

import logging

logging.basicConfig()

logger = logging.getLogger('apscheduler')

logger.setLevel(logging.DEBUG)


app = Flask(__name__)

executors = {
    'default': ThreadPoolExecutor(1)
}

scheduler = BackgroundScheduler(executors=executors)

scheduler.add_jobstore(
    'redis', jobs_key='scheduler.jobs', run_times_key='scheduler.run_times',
    host=REDIS_DIALER_SERVER, port=REDIS_DIALER_PORT, db=3
)


GM_CLIENT = gearman.GearmanClient(GEARMAN_JOB_SERVERS)


def schedule_contact(phone_number, id_campaign, id_contact):
    logger.debug(f'Campaign {id_campaign}: calling scheduled agenda for contact {id_contact}')
    message = json.dumps({'contact': [id_contact, id_campaign, phone_number],
                          'id_campaign': id_campaign})
    GM_CLIENT.submit_job('process-contact', message, background=True)
    return 'GD!!!'


def schedule_process_campaign(id_campaign):
    logger.debug(f'Campaign {id_campaign} starting to run from the scheduler')
    message = json.dumps({'id_campaign': id_campaign})
    GM_CLIENT.submit_job('process-campaign', message, background=True)
    return 'GD!!!'


@app.route('/add-agenda/<id_campaign>', methods=['POST'])
def add_agenda(id_campaign):
    datetime_agenda_str = request.get_json().get('datetime_agenda', '')
    # datetime_agenda_str = '19/09/22 13:55:26' ## for example
    datetime_agenda = datetime.strptime(datetime_agenda_str, '%d/%m/%y %H:%M:%S')
    phone_number = request.get_json().get('phone_number', '')
    id_contact = request.get_json().get('id_contact', '')
    schedule_type = request.get_json().get('type', '')
    scheduler.add_job(
        schedule_contact, 'date', run_date=datetime_agenda,
        args=[phone_number, id_campaign, id_contact],
        name=schedule_type
    )
    return json.dumps({'msg': 'Contact was scheduled'})


@app.route('/add-process-campaign/<id_campaign>', methods=['POST'])
def add_process_campaign(id_campaign):
    datetime_start_str = request.get_json().get('datetime_start', '')
    datetime_start_campaign = datetime.strptime(datetime_start_str, '%d/%m/%y %H:%M:%S')
    name = 'scheduled_process_campaign_{id_campaign}'
    scheduler.add_job(
        schedule_process_campaign, 'date', run_date=datetime_start_campaign,
        args=[id_campaign],
        name=name
    )
    return json.dumps({'msg': 'Contact was scheduled'})


scheduler.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1441, debug=True)
