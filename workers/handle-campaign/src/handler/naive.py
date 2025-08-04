# -*- coding: utf-8 -*-

from .basic import DialerWorker
from .ari_manager import ARI
from .utils import timed_lru_cache

import re
import json
import os
import redis
import psycopg
import gearman.client
import requests
import datetime
import time

from datetime import timedelta
from decimal import Decimal
from time import sleep

from settings.default import REDIS_DIALER_PORT, REDIS_DIALER_SERVER, GEARMAN_JOB_SERVERS

import logging

from ui.rendering import AdminRender

LOGLEVEL = os.environ.get('PYTHON_LOGLEVEL', 'INFO').upper()

logger = logging.getLogger(__name__)

logging.basicConfig(level=LOGLEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ASTERISK_USER = os.getenv('ASTERISK_USER', 'omnileadsami')

ASTERISK_PASS = os.getenv('ASTERISK_PASS', '5_MeO_DMT')

ASTERISK_HOST = os.getenv('ASTERISK_HOST', 'dialer_acd')

ASTERISK_PORT = os.getenv('ASTERISK_PORT', '8888')

ASTERISK_APP = os.getenv('ASTERISK_APP', 'call_manager')

ARI_BASE_URL = f'http://{ASTERISK_HOST}:{ASTERISK_PORT}/ari'

REDIS_OML_SERVER = os.getenv('REDIS_OML_SERVER', 'oml-redis')

REDIS_OML_PORT = os.getenv('REDIS_OML_PORT', '6379')

POSTGRES_OML_SERVER = os.getenv('POSTGRES_OML_SERVER', 'oml-postgres')

POSTGRES_OML_PORT = os.getenv('POSTGRES_OML_PORT', '5432')

POSTGRES_OML_PASSWORD = os.getenv('POSTGRES_OML_PASSWORD')

POSTGRES_OML_USER = 'omnileads'

POSTGRES_OML_DB = 'omnileads'

POSTGRES_DIALER_SERVER = os.getenv('POSTGRES_DIALER_SERVER', 'dialer-postgres')

POSTGRES_DIALER_PORT = os.getenv('POSTGRES_DIALER_PORT', '5433')

POSTGRES_DIALER_USER = os.getenv('POSTGRES_DIALER_USER', 'omnidialer')

POSTGRES_DIALER_DB = os.getenv('POSTGRES_DIALER_DB', 'omnidialer')

POSTGRES_DIALER_PASSWORD = os.getenv('POSTGRES_DIALER_PASSWORD')

DIALER_ACD_HOST = os.getenv('DIALER_ACD_HOST', 'omlacd')

SCHEDULER_API_HOST = os.getenv('SCHEDULER_API_HOST', 'scheduler-api')

WEEK_DAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']

CAPS = int(os.getenv('CAPS', 3))

# campaign status possible values
CREATED = 1                     # ESTADO_INACTIVA in OML
ACTIVE = 2                      # ESTADO_ACTIVA in OML
PAUSED = 5                      # ESTADO_PAUSADA in OML
FINALIZED = 3                   # ESTADO_FINALIZADA in OML

CAMPAIGN_STATUS_TO_NAME = {
    1: "INACTIVE",
    2: "ACTIVE",
    5: "PAUSED",
    3: "FINALIZED"
}

AVAILABLE_NEXT_STATUSES = {
    CREATED: [ACTIVE, FINALIZED],
    ACTIVE: [PAUSED, FINALIZED],
    PAUSED: [ACTIVE, FINALIZED],
    FINALIZED: [ACTIVE]
}

# contact status, in sync with OML's incidence_rules statuses
# TODO: see the remaining statuses
STATUS_CREATED = 12
STATUS_SELECTED_CALL = 15
STATUS_ANSWERED_AGENT = 6
STATUS_ANSWERED_PSTN = 7
STATUS_BUSY = 1
STATUS_NOANSWER = 3
STATUS_CONGESTION = 4
STATUS_TERMINATED = 2
STATUS_TIMEOUT = 5
STATUS_CHANUNAVAIL = 8

NAME_TO_STATUS = {
    "CHANUNAVAIL": STATUS_CHANUNAVAIL,
    "BUSY": STATUS_BUSY,
    "NOANSWER": STATUS_NOANSWER,
    "CONGESTION": STATUS_CONGESTION,
    "ANSWERED_PSTN": STATUS_ANSWERED_PSTN,
    "ANSWERED_AGENT": STATUS_ANSWERED_AGENT,
    "TERMINATED": STATUS_TERMINATED,
    "TIMEOUT": STATUS_TIMEOUT,
}

# contact final status
INITIAL = 0
PENDING_ATTEMPTS = 1
FINALIZED_NOCONTACT = 2
FINALIZED_SUCCESS = 3

FINAL_STATUS_TO_NAME = {
    FINALIZED_NOCONTACT: "FINALIZED WITH NO CONTACT",
    PENDING_ATTEMPTS: "NO CONTACTS WITH PENDING ATTEMPTS",
    FINALIZED_SUCCESS: "CONTACTED SUCCESSFULLY"
}

NO_DISPOSITION_OPTION = -1

# percentage called threshold for notify OML
PERCENTAGE_PENDING_CALL_THRESHOLD = 5


# status types
PHONE_TYPE = 1
DISPOSITION_TYPE = 2

# fail statuses
# TODO: incorporate the names of the other fail events
FAIL_EVENTS = ['BUSY', 'NOANSWER', 'CONGESTION', 'TIMEOUT', 'TERMINATED']

# fail statuses with no incidence rules
FAIL_NO_RULES_EVENTS = ['CHANUNAVAIL']

# incidence rules multinum behauviour
FIXED = 1
MULT = 2


AVAILABLE_NEXT_STATUSES = {
    CREATED: [ACTIVE, FINALIZED],
    ACTIVE: [PAUSED, FINALIZED],
    PAUSED: [ACTIVE, FINALIZED],
    FINALIZED: [ACTIVE]
}

JOB_STARTED = 1
JOB_FAILED = 2


def job_handler_decorator(method):
    def wrapper(*args, **kwargs):
        worker_class = args[0]
        job = args[2]
        id_job = worker_class.insert_job(job)
        try:
            result = method(*args, **kwargs)
            worker_class.remove_job(id_job)
            return result
        except Exception as e:
            logger.exception(f"An error occurred in {method.__name__}: {e}")
            worker_class.save_job_error(id_job, str(e))
            raise e
    return wrapper


class AverageWorker(DialerWorker):
    """A worker flow with a dialing strategy, call contacts according to the available agents and
    the campaigns they are assigned to"""

    POSTGRES_OML_CONNECTION_STR = (f'postgresql://{POSTGRES_OML_USER}:{POSTGRES_OML_PASSWORD}'
                                   f'@{POSTGRES_OML_SERVER}:{POSTGRES_OML_PORT}/{POSTGRES_OML_DB}')
    POSTGRES_DIALER_CONNECTION_STR = (f'postgresql://{POSTGRES_DIALER_USER}:'
                                      f'{POSTGRES_DIALER_PASSWORD}@{POSTGRES_DIALER_SERVER}:'
                                      f'{POSTGRES_DIALER_PORT}/{POSTGRES_DIALER_DB}')
    REDIS_OML_CONNECTION = None
    REDIS_DIALER_CONNECTION = None
    GM_CLIENT = gearman.GearmanClient(GEARMAN_JOB_SERVERS)

    ari = ARI(
        user=ASTERISK_USER,
        password=ASTERISK_PASS,
        host=ASTERISK_HOST,
        port=int(ASTERISK_PORT)
    )

    @classmethod
    def insert_job(cls, job):
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn:
            cursor = conn.cursor()
            job_unique = job.unique.decode('utf8')
            job_name = job.task.decode('utf8')
            cursor.execute(
                'INSERT INTO jobs (job_id, job_name, status) VALUES (%s, %s, %s) RETURNING id;',
                (job_unique, job_name, JOB_STARTED))
            return cursor.fetchone()[0]

    @classmethod
    def save_job_error(cls, id_job, exception):
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE jobs SET status = %s, error = %s WHERE id = %s;',
                           (JOB_FAILED, exception, id_job))

    @classmethod
    def remove_job(cls, id_job):
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM jobs WHERE id = %s;', (id_job,))

    @classmethod
    def system_is_active(cls):
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT is_active FROM system_control;')
            return cursor_dialer.fetchone()[0]

    @classmethod
    def process_campaign_inside(cls, id_campaign):
        while cls.campaign_is_active(id_campaign):
            logger.debug(f'\nCampaign {id_campaign} is active')
            allowed_to_call, extra_info = cls.is_allowed_to_call(id_campaign)
            if allowed_to_call:
                logger.debug(f'Campaign {id_campaign} is allowed to call')
                contacts_attempts_number = cls.allowed_parallel_contact_attempts(id_campaign)
                initial_time = datetime.datetime.now()
                caps_calls_counter = 0
                contacts = cls.take_contacts(contacts_attempts_number, id_campaign)
                for contact in contacts:
                    while True:
                        # we need to ensure the selected contact is eventually called
                        current_time = datetime.datetime.now()
                        current_delta = current_time - initial_time
                        if current_delta >= timedelta(seconds=1):
                            logger.debug(f"Campaign {id_campaign}: CAPS init")
                            caps_calls_counter = 0
                            initial_time = current_time
                        else:
                            if caps_calls_counter < CAPS:
                                logger.debug(
                                    f"Campaign {id_campaign}: attempt to call selected contact")
                                cls.attempt_contact(contact, id_campaign)
                                caps_calls_counter += 1
                                break
                            else:
                                logger.debug(f"Campaign {id_campaign}: CAPS sleep")
                                remaining = (timedelta(seconds=1) - current_delta).total_seconds()
                                sleep(remaining)
            else:
                # schedule process-campaign for the next time the campaign is allowed to run
                next_allowed_date = cls.get_next_allowed_date(id_campaign, extra_info)
                data = {'datetime_start': next_allowed_date}
                host = SCHEDULER_API_HOST
                uri = f'http://{host}/add-process-campaign/{id_campaign}'
                requests.post(uri, json=data)
                return None

    @classmethod
    def get_next_day_of_week_allowed(
            cls, permission_days_campaign, day_of_week, current_date, hour_start):
        # get next day of the week allowed in the campaign
        # search for an allowed day
        dow = permission_days_campaign[(day_of_week + 1) % 6]
        while not dow:
            dow = (dow + 1) % 6
        # construct the datetime
        days_until_next_day_allowed = (dow - day_of_week) % 7
        date = current_date + timedelta(days_until_next_day_allowed)
        return datetime.combine(date, hour_start)

    @classmethod
    def get_next_allowed_date(cls, id_campaign, extra_info):
        (day_of_week_allowed, day_of_week, hour_match, current_date, hour,
         minute, campaign_info) = extra_info
        (hour_start, hour_end, monday, tuesday, wednesday, thursday, friday, saturday,
         sunday) = campaign_info
        permission_days_campaign = campaign_info[:2]
        # if the day of the week is not allowed get the next day of week allowed with hour_start
        if not day_of_week_allowed:
            return cls.get_next_day_of_week_allowed(day_of_week, current_date, hour_start)
        # if current time < hour_start, just use the same day with hour_start
        # else, get the next day of week allowed with hour start
        current_time = datetime.time(hour, minute)
        if current_time < hour_start:
            return datetime.combine(current_date, hour_start)
        return cls.get_next_day_of_week_allowed(
            permission_days_campaign, day_of_week, current_date, hour_start)

    @classmethod
    def connect_redis_oml(cls):
        if cls.REDIS_OML_CONNECTION is None:
            cls.REDIS_OML_CONNECTION = redis.Redis(
                host=REDIS_OML_SERVER, port=REDIS_OML_PORT, decode_responses=True, db=0)

    @classmethod
    def connect_redis_dialer(cls):
        if cls.REDIS_DIALER_CONNECTION is None:
            cls.REDIS_DIALER_CONNECTION = redis.Redis(
                host=REDIS_DIALER_SERVER, port=REDIS_DIALER_PORT, decode_responses=True, db=3)

    @classmethod
    def is_allowed_to_call(cls, id_campaign):
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            return cls.opening_hours_match(cursor_dialer, id_campaign)

    @classmethod
    def get_campaign_data(cls, id_campaign, cursor_oml, contact_strategy):
        # import campaign configuration from tables of OML
        logger.debug(f'Retrieving data from OML campaign with id={id_campaign}')
        logger.debug(f'Campaign {id_campaign}: from ominicontacto_app_campana')
        cursor_oml.execute(
            'SELECT id,estado,nombre,fecha_inicio,fecha_fin,control_de_duplicados,prioridad '
            ' FROM ominicontacto_app_campana WHERE id = %s;', (id_campaign,))
        campaign_id_data = cursor_oml.fetchone()
        logger.debug(f'Campaign {id_campaign}: from queue_table')
        cursor_oml.execute(
            'SELECT strategy,wait,initial_predictive_model,initial_boost_factor,maxlen '
            ' FROM queue_table WHERE campana_id = %s;', (id_campaign,))
        campaign_id_data += cursor_oml.fetchone()
        logger.debug(f'Campaign {id_campaign}: from ominicontacto_app_actuacionvigente')
        cursor_oml.execute('SELECT domingo,lunes,martes,miercoles,jueves,viernes,sabado,hora_desde,'
                           'hora_hasta FROM ominicontacto_app_actuacionvigente'
                           ' WHERE campana_id = %s;', (id_campaign,))
        campaign_id_data += cursor_oml.fetchone()
        logger.debug(f'Campaign {id_campaign}: setting dialer specific options')
        campaign_id_data += (contact_strategy, CREATED)
        logger.debug(f'Campaign {id_campaign}: from incidence rules')
        cursor_oml.execute(
            'SELECT * FROM ominicontacto_app_reglasincidencia WHERE campana_id = %s;',
            (id_campaign,))
        incidence_rules_data = cursor_oml.fetchall()
        logger.debug(f'Campaign {id_campaign}: from incidence rules for disposition options')
        cursor_oml.execute(
            """SELECT ric.id,ric.opcion_calificacion_id,ric.intento_max,ric.reintentar_tarde,
            ric.en_modo,opc.campana_id
            FROM ominicontacto_app_reglaincidenciaporcalificacion AS ric
            INNER JOIN ominicontacto_app_opcioncalificacion AS opc ON
            ric.opcion_calificacion_id = opc.id
            AND opc.campana_id = %s;""", (id_campaign,))
        incidence_rules_disposition_data = cursor_oml.fetchall()
        cursor_oml.execute(
            """SELECT db.metadata FROM ominicontacto_app_basedatoscontacto as db
            INNER JOIN ominicontacto_app_campana AS ca ON ca.bd_contacto_id = db.id
            AND ca.id = %s;""", (id_campaign,))
        metadata = json.dumps(cursor_oml.fetchone()[0])
        campaign_id_data += (metadata,)
        cls.connect_redis_oml()
        customdialerdst = cls.REDIS_OML_CONNECTION.hget(
            f'OML:CAMP:{id_campaign}', 'CUSTOMDIALERDST')
        campaign_id_data += (customdialerdst,)
        return campaign_id_data, incidence_rules_data, incidence_rules_disposition_data

    @classmethod
    @job_handler_decorator
    def edit_campaign(cls, worker, job):
        # assumes the dialer campaign exists in OML with all the required tables and fields created
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        logger.debug(f'Editing the campaign {id_campaign}')
        contact_strategy = data['contact_strategy']
        # 0- pause campaign
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                orig_status_campaign = cls.get_campaign_status(id_campaign, cursor_dialer)
                cls.set_campaign_status(id_campaign, PAUSED, cursor_dialer)
                with psycopg.connect(cls.POSTGRES_OML_CONNECTION_STR) as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    (campaign_id_data, incidence_rules_data,
                     incidence_rules_disposition_data) = cls.get_campaign_data(
                        id_campaign, cursor_oml, contact_strategy)
                    # 1- update campaign table
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the campaign data into omnidialer')
                    cls.connect_redis_dialer()
                    customdialerdst = campaign_id_data[-1]
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:CUSTOMDIALERDST', customdialerdst)
                    params = campaign_id_data[1:] + (id_campaign,)
                    cursor_dialer.execute(
                        """UPDATE campaign SET oml_status = %s, name = %s, start_date = %s,
                        end_date = %s, duplicates_control = %s, priority = %s, strategy = %s,
                        wait = %s, initial_predictive_model = %s, initial_boost_factor = %s,
                        max_channels = %s, sunday = %s, monday = %s, tuesday = %s, wednesday = %s,
                        thursday = %s, friday = %s, saturday = %s, hour_start = %s, hour_ends = %s,
                        contact_strategy = %s, dialer_status = %s, metadata = %s,
                        customdialerdst = %s
                        WHERE id = %s;""", params)
                    # 2- update incidence rules
                    cursor_dialer.execute('DELETE FROM incidence_rules WHERE campaign_id = %s',
                                          (id_campaign,))
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the incidence_rules into omnidialer')
                    for incidence_rule in incidence_rules_data:
                        cursor_dialer.execute("""INSERT INTO incidence_rules
                         (id, status, status_custom, max_attempt, retry_later, in_mode, campaign_id)
                         VALUES (%s, %s, %s, %s, %s, %s, %s);""", incidence_rule)
                    cursor_dialer.execute("""DELETE FROM incidence_rules_disposition
                    WHERE campaign_id = %s""", (id_campaign,))
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the incidence_rules for disposition'
                        ' into omnidialer')
                    for incidence_rule in incidence_rules_disposition_data:
                        cursor_dialer.execute("""INSERT INTO incidence_rules_disposition
                        (id, disposition_option_id, max_attempt, retry_later, in_mode, campaign_id)
                        VALUES (%s, %s, %s, %s, %s, %s);""", incidence_rule)
                if orig_status_campaign == ACTIVE:
                    cls.set_campaign_status(id_campaign, ACTIVE, cursor_dialer)
                    message = json.dumps({'id_campaign': id_campaign})
                    cls.GM_CLIENT.submit_job('process-campaign', message, background=True)

        response = f'Campaign {id_campaign} with strategy {contact_strategy} succesfully updated!!!'

        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    def get_contacts_campaign(cls, cursor, size):
        return cursor.fetchmany(size=size)

    @classmethod
    def copy_contacts_from_oml(cls, cursor_dialer, cursor_oml, id_campaign):
        logger.debug(f'Campaign {id_campaign}: retrieving the contacts')
        sql = """SELECT co.id, co.telefono, co.datos, co.es_originario
        FROM ominicontacto_app_contacto AS co
        INNER JOIN ominicontacto_app_basedatoscontacto AS db ON
        db.id = co.bd_contacto_id INNER JOIN ominicontacto_app_campana
        AS ca ON db.id = ca.bd_contacto_id AND ca.id = %s;"""
        size = 1000
        logger.debug(f'Campaign {id_campaign}: copying the contacts')
        cursor_oml.execute(sql, (id_campaign,))
        while True:
            contacts = cls.get_contacts_campaign(cursor_oml, size)
            if not contacts:
                break
            for (id_contact, phone, data, is_original) in contacts:
                if phone != '':
                    cursor_dialer.execute(
                        'INSERT INTO contact '
                        '(id, phone, data, is_original) VALUES (%s, %s, %s, %s)'
                        'ON CONFLICT (id) DO NOTHING;',
                        (id_contact, phone, data, is_original))
                    cursor_dialer.execute(
                        """INSERT INTO contact_in_campaign (id_campaign, id_contact, status,
                    final_status, disposition_option) VALUES (%s, %s, %s, %s, %s);""",
                        (id_campaign, id_contact, STATUS_CREATED, INITIAL,
                         NO_DISPOSITION_OPTION))

    @classmethod
    @job_handler_decorator
    def create_campaign(cls, worker, job):
        # assumes the dialer campaign exists in OML with all the required tables and fields created
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        logger.debug(f'Creating the campaign {id_campaign}')
        contact_strategy = data['contact_strategy']
        prefix = data['prefix']
        cls.connect_redis_dialer()
        cls.connect_redis_oml()
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with psycopg.connect(cls.POSTGRES_OML_CONNECTION_STR) as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    (campaign_id_data, incidence_rules_data,
                     incidence_rules_disposition_data) = cls.get_campaign_data(
                        id_campaign, cursor_oml, contact_strategy)
                    cls.connect_redis_dialer()
                    customdialerdst = campaign_id_data[-1]
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:CUSTOMDIALERDST', customdialerdst)
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the campaign data into omnidialer')
                    campaign_id_data = campaign_id_data + (prefix,)
                    cursor_dialer.execute(
                        """INSERT INTO campaign (id, oml_status, name, start_date, end_date,
                        duplicates_control, priority, strategy, wait, initial_predictive_model,
                        initial_boost_factor, max_channels, sunday, monday, tuesday, wednesday,
                        thursday, friday, saturday, hour_start, hour_ends, contact_strategy,
                        dialer_status, metadata, customdialerdst, prefix) VALUES
                         (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s);""", campaign_id_data)
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the incidence_rules into omnidialer')
                    for incidence_rule in incidence_rules_data:
                        cursor_dialer.execute(
                            """INSERT INTO incidence_rules
                            (id, status, status_custom, max_attempt, retry_later, in_mode,
                            campaign_id) VALUES (%s, %s, %s, %s, %s, %s, %s);""", incidence_rule)
                    logger.debug(f'Campaign {id_campaign}: Inserting the incidence_rules for'
                                 ' disposition option into omnidialer')
                    for incidence_rule in incidence_rules_disposition_data:
                        cursor_dialer.execute(
                            """INSERT INTO incidence_rules_disposition
                            (id, disposition_option_id, max_attempt, retry_later, in_mode,
                            campaign_id) VALUES (%s, %s, %s, %s, %s, %s);""", incidence_rule)
                    cls.copy_contacts_from_oml(cursor_dialer, cursor_oml, id_campaign)
                    cls.REDIS_DIALER_CONNECTION.set(f'OML:CALLS:{id_campaign}:DIALER', 0)
                    cls.REDIS_OML_CONNECTION.publish(
                        'OML:CHANNEL:DIALER',
                        json.dumps({'type': 'CREATE',
                                    'camp_id': id_campaign}))

        response = f'Campaign {id_campaign} with strategy {contact_strategy} created!!!'

        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    def clean_selected_contacts(cls, id_campaign):
        # set contacts marked as SELECTED_CALL back to
        # CREATED status, so they can be consumed by the process campaign
        # this is due to these contacts were marked and not called
        # or at least we didn't receive events from Asterisk to change their state
        logger.debug(f'Campaign {id_campaign}: cleaning broken selected contacts')
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE contact_in_campaign SET status = %s WHERE'
                ' id_campaign = %s and status = %s;',
                (STATUS_CREATED, id_campaign, STATUS_SELECTED_CALL))
            row_count = cursor.rowcount
            if row_count > 0:
                logger.debug(
                    f"Campaign {id_campaign}: cleaned broken selected contacts={row_count}")

    @classmethod
    @job_handler_decorator
    def start_campaign(cls, worker, job):
        cls.connect_redis_oml()
        if not cls.system_is_active():
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({'type': 'SYSTEM_STOPPED',
                            'camp_id': 'all'}))
            return b'Forbidden operation'
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        cls.clean_selected_contacts(id_campaign)
        logger.debug(f'Campaign {id_campaign}: starting the campaign')
        cls.set_campaign_status(id_campaign, ACTIVE, sync_omnileads=sync_omnileads)
        message = json.dumps({'id_campaign': id_campaign})
        cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        return b'Campaign started!'

    @classmethod
    def opening_hours_match(cls, cursor, id_campaign):
        cursor.execute('SELECT EXTRACT(DOW FROM CURRENT_DATE) AS day_of_week;')
        day_of_week = WEEK_DAYS[int(cursor.fetchone()[0])]
        cursor.execute(f'SELECT {day_of_week} FROM ONLY campaign WHERE id = %s;', (id_campaign,))
        day_of_week_allowed = cursor.fetchone()[0]
        cursor.execute('SELECT * FROM ONLY campaign WHERE id = %s AND CURRENT_TIME BETWEEN'
                       ' hour_start AND hour_ends;', (id_campaign,))
        hour_match = cursor.fetchone()
        if not day_of_week_allowed:
            logger.debug(f'Campaign {id_campaign}: day week not allowed to call')
        elif not hour_match:
            logger.debug(f'Campaign {id_campaign}: in the current time is not allowed to call')
        result = day_of_week_allowed and hour_match
        extra_info = None
        if not result:
            cursor.execute('SELECT EXTRACT(HOUR FROM NOW()) AS current_hour, '
                           'EXTRACT(MINUTE FROM NOW()) AS current_minute;')
            hour, minute = cursor.fetch_one()[0]
            cursor.execute('SELECT hour_start,hour_end,'
                           'monday,tuesday,wednesday,thursday,friday,saturday,sunday'
                           ' FROM campaign where id = %s;', (id_campaign,))
            campaign_info = cursor.fetch_one()[0]
            cursor.execute('SELECT CURRENT_DATE;')
            current_date = cursor.fetch_one()[0]
            extra_info = (day_of_week_allowed, day_of_week, hour_match, current_date, hour,
                          minute, campaign_info)
        return result, extra_info

    @classmethod
    def get_campaign_status(cls, id_campaign, dialer_cursor):
        dialer_cursor.execute(
            'SELECT dialer_status FROM ONLY campaign WHERE id = %s', (id_campaign,))
        return dialer_cursor.fetchone()[0]

    @classmethod
    def all_contacts_were_attempted(cls, id_campaign):
        cls.connect_redis_dialer()
        pending_initial_contact_attempts = cls.REDIS_DIALER_CONNECTION.hget(
            f'CAMP:{id_campaign}:COUNTER',
            'PENDING_INITIAL_CONTACT_ATTEMPTS') or -1
        return int(pending_initial_contact_attempts) == 0

    @classmethod
    def no_active_incidence_rules(cls, id_campaign):
        cls.connect_redis_dialer()
        pending_attempts = cls.REDIS_DIALER_CONNECTION.hget(
            f'CAMP:{id_campaign}:COUNTER',
            FINAL_STATUS_TO_NAME[PENDING_ATTEMPTS]
        )
        return int(pending_attempts) == 0

    @classmethod
    def no_active_agendas(cls):
        cls.connect_redis_dialer()
        len_agendas = AverageWorker.REDIS_DIALER_CONNECTION.zrange("scheduler.run_times", 0, -1)
        return len_agendas == []

    @classmethod
    def campaign_is_active(cls, id_campaign):
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            status = cls.get_campaign_status(id_campaign, cursor_dialer)
            # notify to OML if the campaign is outdated and pause the campaign
            cursor_dialer.execute(
                """SELECT id
                FROM ONLY campaign
                WHERE end_date >= now()::date
                AND start_date <= now()::date
                AND id = %s;""", (id_campaign,))
            campaign_in_range = cursor_dialer.fetchone()
            if not campaign_in_range:
                logger.debug(f'Campaign {id_campaign}: campaign expired')
                cls.set_campaign_status(id_campaign, PAUSED, sync_omnileads=True)
                cls.connect_redis_oml()
                cls.REDIS_OML_CONNECTION.publish(
                    'OML:CHANNEL:DIALER',
                    json.dumps({'type': 'EXPIRATION',
                                'camp_id': id_campaign}))
                return False

            # notify to OML if there are no more contacts for call and pause the campaign
            if cls.all_contacts_were_attempted(id_campaign) and \
               cls.no_active_incidence_rules(id_campaign) and cls.no_active_agendas():
                logger.debug(f'Campaign {id_campaign}: no more contacts pending for call')
                cls.set_campaign_status(id_campaign, FINALIZED, sync_omnileads=True)
                cls.connect_redis_oml()
                cls.REDIS_OML_CONNECTION.publish(
                    'OML:CHANNEL:DIALER',
                    json.dumps({
                        'type': 'CONTACTS_CONSUMED',
                        'camp_id': id_campaign,
                    }))
                return False

            # notify to OML if there are less than PERCENTAGE_PENDING_CALL_THRESHOLD%
            # of contacts pending for call
            # TODO: not sure about the frequency of this notification
            cursor_dialer.execute(
                """SELECT status, count(*) * 100.0 / (SELECT count(*) FROM contact_in_campaign)
                FROM ONLY contact_in_campaign
                WHERE status = %s AND id_campaign = %s
                GROUP BY status;""", (STATUS_ANSWERED_AGENT, id_campaign))
            percentage_called = cursor_dialer.fetchone()
            percentage_called = percentage_called[1] if percentage_called is not None else 0
            percentage_pending_call = 100 - percentage_called
            if percentage_pending_call <= PERCENTAGE_PENDING_CALL_THRESHOLD:
                logger.debug(f'Campaign {id_campaign}: less than '
                             f'{PERCENTAGE_PENDING_CALL_THRESHOLD}% contacts pending for call')
                cls.connect_redis_oml()
                cls.REDIS_OML_CONNECTION.publish(
                    'OML:CHANNEL:DIALER',
                    json.dumps({
                        'type': 'ALMOST_NO_CONTACTS',
                        'camp_id': id_campaign,
                    }))
        return status == ACTIVE

    @classmethod
    def get_number_active_campaigns(cls):
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT Count(*) FROM ONLY campaign WHERE dialer_status = %s""",
                (ACTIVE,))
            active_campaigns = cursor.fetchone()[0]
        return active_campaigns

    @classmethod
    def get_agent_ids_campaign(cls, id_campaign):
        with psycopg.connect(cls.POSTGRES_OML_CONNECTION_STR) as conn_oml:
            cursor_oml = conn_oml.cursor()
            TYPE_DIALER = 2
            STATUS_ACTIVE = 2
            cursor_oml.execute(
                """SELECT ominicontacto_app_agenteprofile.id, COUNT(queue_table.campana_id)
                AS queue__campana__count
                FROM ominicontacto_app_agenteprofile LEFT OUTER JOIN queue_member_table ON
                (ominicontacto_app_agenteprofile.id = queue_member_table.member_id)
                LEFT OUTER JOIN queue_table ON (queue_member_table.queue_name = queue_table.name)
                LEFT OUTER JOIN ominicontacto_app_campana ON
                (queue_table.campana_id = ominicontacto_app_campana.id)
                WHERE ominicontacto_app_agenteprofile.id in
                (
                SELECT ominicontacto_app_agenteprofile.id FROM ominicontacto_app_agenteprofile
                INNER JOIN queue_member_table ON
                (ominicontacto_app_agenteprofile.id = queue_member_table.member_id)
                INNER JOIN queue_table ON (queue_member_table.queue_name = queue_table.name)
                WHERE queue_table.campana_id = %s
                ) AND ominicontacto_app_campana.type = %s AND ominicontacto_app_campana.estado = %s
                GROUP BY ominicontacto_app_agenteprofile.id;""",
                (id_campaign, TYPE_DIALER, STATUS_ACTIVE))
            return dict(cursor_oml.fetchall())

    @classmethod
    def get_number_available_agents(cls, id_campaign):
        cls.connect_redis_oml()
        agents_distribution = cls.get_agent_ids_campaign(id_campaign)
        agents_available = 0
        total_agents_available = 0
        for key in cls.REDIS_OML_CONNECTION.scan_iter(match='OML:AGENT:*', count=1000):
            id_agent = int(key.split(':')[-1])
            if agents_distribution.get(id_agent):
                status = cls.REDIS_OML_CONNECTION.hget(key, 'STATUS')
                if status == 'READY':
                    agents_available += (1 / agents_distribution[id_agent])
                    total_agents_available += 1
        if agents_available < 1:
            if agents_available > 0:
                # there is at least one agent active
                return 1, total_agents_available
            return 0, total_agents_available
        return int(agents_available), total_agents_available

    @classmethod
    def get_active_channels(cls, id_campaign):
        cls.connect_redis_oml()
        active_channels = cls.REDIS_DIALER_CONNECTION.get(f'OML:CALLS:{id_campaign}:DIALER')
        if active_channels is None:
            logger.debug(
                f'OML:CALLS:{id_campaign}:DIALER key not available, check your OML installation')
            return -1
        return int(active_channels)

    @classmethod
    def get_campaign_max_available_channels(cls, id_campaign):
        # TODO: consider some caching here?
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT max_channels FROM ONLY campaign WHERE id = %s;',
                                  (id_campaign,))
            return cursor_dialer.fetchone()[0]

    @classmethod
    def get_allowed_attempts_according_agents(cls, id_campaign, active_channels,
                                              campaign_max_available_channels):
        available_agents, total_available_agents = cls.get_number_available_agents(id_campaign)
        active_campaigns = cls.get_number_active_campaigns()
        logger.debug("Campaign {0}: active_campaigns={1}".format(id_campaign, active_campaigns))
        logger.debug("Campaign {0}: available_agents={1}".format(id_campaign, available_agents))
        logger.debug("Campaign {0}: total_available_agents={1}".format(
            id_campaign, total_available_agents))
        if active_channels < campaign_max_available_channels:
            if total_available_agents >= active_channels:
                if active_campaigns > 0:
                    # TODO: figure out how to get back to this heuristic when the agents
                    # are assigned to the same campaign
                    # return available_agents / active_campaigns
                    return available_agents
                return 0
            logger.debug(f"Campaign {id_campaign}: too much calls for available agents")
            return 0
        return 0

    @classmethod
    def allowed_parallel_contact_attempts(cls, id_campaign):
        cls.connect_redis_dialer()
        active_channels = cls.get_active_channels(id_campaign)
        if active_channels == -1:
            return 0
        campaign_max_available_channels = cls.get_campaign_max_available_channels(id_campaign)
        num_available_channels = campaign_max_available_channels - active_channels
        # ensure num_available_channels is not affected by changes in the config
        # in the middle of the campaign process
        num_available_channels = max(num_available_channels, 0)
        logger.debug("Campaign {0}: active_channels={1}".format(id_campaign, active_channels))
        logger.debug("Campaign {0}: campaign_max_available_channels={1}".format(
            id_campaign, campaign_max_available_channels))
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT initial_boost_factor FROM ONLY campaign WHERE id = %s',
                                  (id_campaign,))
            boost_factor = cursor_dialer.fetchone()[0]
            if cls.REDIS_DIALER_CONNECTION.get(f'CAMP:{id_campaign}:CUSTOMDIALERDST') == '0':
                allowed_parallel_attempts_acc_agents = int(Decimal(
                    cls.get_allowed_attempts_according_agents(
                        id_campaign, active_channels,
                        campaign_max_available_channels)) * boost_factor)
                return min(num_available_channels, allowed_parallel_attempts_acc_agents)
            return num_available_channels

    @classmethod
    def take_contacts(cls, contacts_attempts_number, id_campaign):
        logger.debug("Campaign {0}: contacts_attempts_number={1}".format(
            id_campaign, contacts_attempts_number))
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("""UPDATE contact_in_campaign as cc
                                     SET status = %s,
                                     schedule_aborted = false
                                     FROM contact as co
                                     WHERE cc.id IN (SELECT id
                                     FROM ONLY contact_in_campaign
                                     WHERE id_campaign = %s and
                                     (status = %s OR schedule_aborted = true)
                                     LIMIT %s) AND co.id = cc.id_contact
                                     RETURNING cc.id_contact, cc.id_campaign, co.phone;""",
                                  (STATUS_SELECTED_CALL, id_campaign, STATUS_CREATED,
                                   contacts_attempts_number))
            contacts = cursor_dialer.fetchall()
            logger.debug("Campaign {0}: selected {1} contacts".format(id_campaign, len(contacts)))
            return contacts

    @classmethod
    def attempt_contact(cls, contact, id_campaign):
        message = json.dumps({'contact': contact, 'id_campaign': id_campaign})
        # TODO: think if the following could be a background job call
        cls.GM_CLIENT.submit_job('process-contact', message)

    @classmethod
    @job_handler_decorator
    def process_contact(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        contact = data['contact']
        id_contact = contact[0]
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            status_campaign = cls.get_campaign_status(id_campaign, cursor_dialer)
            if status_campaign == ACTIVE:
                if cls.is_allowed_to_call(id_campaign):
                    logger.debug(
                        f'Attempting to make a contact in campaign {id_campaign} '
                        f'to contact {id_contact}')
                    cls.attempt_contact_asterisk(contact, id_campaign)
                    cls.connect_redis_dialer()
                    cls.REDIS_DIALER_CONNECTION.hincrby(
                        f'CAMP:{id_campaign}:COUNTER',
                        'ATTEMPTED_CALLS',
                    )
                    return b'Contact was called'
                else:
                    logger.debug(
                        f'Campaign {id_campaign} is not allowed to call at the moment, '
                        f'aborting call to contact {id_contact}')
            elif status_campaign == PAUSED:
                logger.debug(
                    f'Campaign {id_campaign} is paused, aborting call to contact {id_contact}')
            else:
                logger.debug(
                    f'Campaign {id_campaign} is finalized, aborting call to contact {id_contact}')
                # status_campaign == FINALIZED
            cursor_dialer.execute('UPDATE contact_in_campaign SET schedule_aborted = true'
                                  ' WHERE id_contact = %s AND id_campaign = %s',
                                  (id_contact, id_campaign))
            return b'Aborted call, campaign is not active'

    @classmethod
    def attempt_contact_asterisk(cls, contact_info, id_campaign):
        logger.debug(f'Campaign {id_campaign}: trying to call the contact')
        id_customer = contact_info[0]
        phone_number = contact_info[2]
        prefix = cls.get_prefix(id_campaign)
        if prefix is not None:
            phone_number = prefix[0] + phone_number

        # callid generate
        epoch = int(time.time())
        callid = f"{epoch}.{contact_info[0]}"

        queue_timeout = 20
        channel_type = 'to_omlacd_dialout'
        caller_id = f'{id_campaign}_{id_customer}_{phone_number}'
        variables = {
            'PJSIP_HEADER(add,OMLCODCLI)': f'{id_customer}',
            'PJSIP_HEADER(add,OMLCAMPID)': f'{id_campaign}',
            'PJSIP_HEADER(add,OMLOUTNUM)': f'{phone_number}',
            'PJSIP_HEADER(add,OMLUNIQUEID)': f'{callid}',
        }
        call_type = 2
        endpoint = f'PJSIP/{phone_number}@{DIALER_ACD_HOST}'
        appArgs = f"""id_camp: {id_campaign}, id_customer: {id_customer},
    tel_customer: {phone_number}, queue_timeout: {queue_timeout},
    channel_type: {channel_type}, call_type: {call_type}, uniqueid: {callid}"""

        logger.debug(f"""Calling contact {id_customer} with phone {phone_number}
    in campaign {id_campaign} using callid {callid}""")

        response = cls.ari.originate_channel(
            endpoint=endpoint,
            app=ASTERISK_APP,
            callerId=caller_id,
            appArgs=appArgs,
            variables=variables
        )
        logger.debug(response)

    @classmethod
    @job_handler_decorator
    def pause_campaign(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        logger.debug(f'Campaign {id_campaign} pausing the campaign')
        cls.set_campaign_status(id_campaign, PAUSED, sync_omnileads=sync_omnileads)
        response = f'Campaign {id_campaign} was paused!'
        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    @job_handler_decorator
    def resume_campaign(cls, worker, job):
        cls.connect_redis_oml()
        if not cls.system_is_active():
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({'type': 'SYSTEM_STOPPED',
                            'camp_id': 'all'}))
            return b'Forbidden operation'
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        cls.clean_selected_contacts(id_campaign)
        logger.debug(f'Campaign {id_campaign} resuming the campaign')
        cls.set_campaign_status(id_campaign, ACTIVE, sync_omnileads=sync_omnileads)
        message = json.dumps({'id_campaign': id_campaign})
        cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        response = f'Campaign {id_campaign} was resumed!'
        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    @job_handler_decorator
    def process_campaign(cls, worker, job):
        id_campaign = cls.decode_payload(job.data)['id_campaign']
        logger.debug(f'Campaign {id_campaign}: resuming the campaign')
        cls.process_campaign_inside(id_campaign)
        response = f'Campaign {id_campaign} process ended!'
        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_prefix(cls, id_campaign):
        logger.debug(f'Campaign {id_campaign}: getting prefix')
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT prefix FROM campaign WHERE'
                ' id = %s;',
                (id_campaign,))
            return cursor_dialer.fetchone()

    @classmethod
    @timed_lru_cache(seconds=6000, maxsize=128)
    def get_incidence_rule(cls, id_campaign, status):
        logger.debug(f'Campaign {id_campaign}: getting the incidence rule for {status}')
        status_code = NAME_TO_STATUS[status]
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT retry_later, max_attempt, in_mode FROM ONLY incidence_rules WHERE'
                ' campaign_id = %s AND status = %s;',
                (id_campaign, status_code))
            return cursor_dialer.fetchone()

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_incidence_rule_disposition(cls, id_campaign, disposition_option):
        logger.debug(f'Campaign {id_campaign}: getting the incidence rule for '
                     f'disposition {disposition_option}')
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT retry_later, max_attempt, in_mode FROM ONLY incidence_rules_disposition'
                ' WHERE campaign_id = %s AND disposition_option_id = %s;',
                (id_campaign, disposition_option))
            return cursor_dialer.fetchone()

    @classmethod
    def get_next_phone_number(cls, cursor_dialer, id_campaign, contact_id, phone_number):
        phone_number_index = cls.REDIS_DIALER_CONNECTION.hget(
            f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_INDEX')
        if phone_number_index is None:
            # first time: the data is only encoded in Postgres (tables campaign & contact)
            # proceding to decode it
            cursor_dialer.execute('SELECT metadata FROM ONLY campaign WHERE id = %s',
                                  (id_campaign,))
            metadata = json.loads(cursor_dialer.fetchone()[0])
            phone_number_indexes = metadata['cols_telefono'][1:]
            cursor_dialer.execute(
                """SELECT co.data FROM ONLY contact_in_campaign AS cc
                INNER JOIN contact AS co on cc.id_contact = co.id
                WHERE cc.id_campaign = %s AND cc.id_contact = %s;""", (id_campaign, contact_id))
            data = json.loads(cursor_dialer.fetchone()[0])
            phone_number_index = 0
            phone_numbers = [phone_number]
            for i in phone_number_indexes:
                phone_numbers.append(data[i - 1])
            cursor_dialer.execute('UPDATE contact_in_campaign SET phone_numbers_list = %s WHERE'
                                  ' id_campaign = %s AND id_contact = %s;',
                                  (phone_numbers, id_campaign, contact_id))
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_LIST',
                json.dumps(phone_numbers))
        else:
            phone_numbers = json.loads(cls.REDIS_DIALER_CONNECTION.hget(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_LIST'))
        phone_number_index = (int(phone_number_index) + 1) % len(phone_numbers)
        # update Postgres & Redis
        cursor_dialer.execute('UPDATE contact_in_campaign SET phone_number_index = %s'
                              ' WHERE id_campaign = %s AND id_contact = %s;',
                              (phone_number_index, id_campaign, contact_id))
        cls.REDIS_DIALER_CONNECTION.hset(
            f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_INDEX', phone_number_index)
        return phone_numbers[phone_number_index]

    @classmethod
    def get_delay(cls, retry_later, type_incidence_rule, attempt_number):
        if type_incidence_rule == FIXED:
            return retry_later
        # MULT
        return retry_later * attempt_number

    @classmethod
    def apply_incidence_rule(
            cls, cursor_dialer, incidence_rule, contact_id, id_campaign, status, status_type,
            phone_number):
        if incidence_rule is not None:
            retry_later, max_attempt, type_incidence_rule = incidence_rule
            contact_history = cls.REDIS_DIALER_CONNECTION.lrange(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}:HISTORY', 0, -1)
            attempt_number = contact_history.count(str((status, status_type)))
            if attempt_number <= max_attempt:
                phone_number = cls.get_next_phone_number(
                    cursor_dialer, id_campaign, contact_id, phone_number)
                retry_later = cls.get_delay(retry_later, type_incidence_rule, attempt_number)
                datetime_retry_later = datetime.datetime.now() + timedelta(
                    seconds=retry_later)
                message = json.dumps({'id_campaign': str(id_campaign),
                                      'id_contact': contact_id,
                                      'phone_number': phone_number,
                                      'datetime_agenda': datetime_retry_later.strftime(
                                          '%d/%m/%y %H:%M:%S'),
                                      'type': 'incidence_rule',
                                      })
                cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s WHERE'
                                      ' id_campaign = %s and id_contact = %s;',
                                      (PENDING_ATTEMPTS, id_campaign, contact_id))
                cls.REDIS_DIALER_CONNECTION.hset(
                    f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', PENDING_ATTEMPTS)
                cls.GM_CLIENT.submit_job('schedule-agenda', message)
                return True
            else:
                cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s'
                                      ' WHERE id_campaign = %s and id_contact = %s;',
                                      (FINALIZED_NOCONTACT, id_campaign, contact_id))
                cls.REDIS_DIALER_CONNECTION.hset(
                    f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', FINALIZED_NOCONTACT)
        else:
            cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s WHERE'
                                  ' id_campaign = %s and id_contact = %s;',
                                  (FINALIZED_NOCONTACT, id_campaign, contact_id))
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', FINALIZED_NOCONTACT)
        return False

    @classmethod
    def handle_incidence_rules(cls, status, id_campaign, contact_id, phone_number):
        # if there is an incidence rule for the status:
        #   if the contact's history and the incidence rule indicates that the
        #   contact must be called again, schedule a call according to the incidence rule
        cls.connect_redis_dialer()
        status_code = NAME_TO_STATUS[status]
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            incidence_rule = cls.get_incidence_rule(id_campaign, status)
            cls.apply_incidence_rule(
                cursor_dialer, incidence_rule, contact_id, id_campaign, status_code,
                PHONE_TYPE, phone_number)

    @classmethod
    @job_handler_decorator
    def process_event(cls, worker, job):
        ari_event_data = cls.decode_payload(job.data)
        id_campaign, contact_id, phone_number = cls.get_contact_data(ari_event_data)
        if cls.is_answer_event(ari_event_data):
            if cls.is_answered_pstn(ari_event_data):
                status = "ANSWERED_PSTN"
                logger.debug(f'Campaign {id_campaign}: receiving answer pstn')
                cls.set_contact_status(id_campaign, contact_id, status)
            elif cls.is_answered_agent(ari_event_data):
                status = "ANSWERED_AGENT"
                logger.debug(f'Campaign {id_campaign}: Receiving answer agent')
                cls.set_contact_status(id_campaign, contact_id, status)
                cls.connect_redis_dialer()
                with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
                    cursor_dialer = conn_dialer.cursor()
                    cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s WHERE'
                                          ' id_campaign = %s and id_contact = %s;',
                                          (FINALIZED_SUCCESS, id_campaign, contact_id))
                    cls.REDIS_DIALER_CONNECTION.hset(
                        f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', FINALIZED_SUCCESS)
                logger.debug(f'Contact {contact_id} was succesfully called to phone {phone_number}'
                             f' in campaign {id_campaign}')
        elif cls.is_fail_event(ari_event_data):
            cls.handle_fail_event(ari_event_data, id_campaign, contact_id, phone_number)
        cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
        return b'Event was processed'

    @classmethod
    def is_fail_event(cls, ari_event_data):
        dialstatus = ari_event_data.get('dialstatus')
        type_event = ari_event_data.get('type')
        return type_event == 'Dial' and  \
            ((dialstatus in FAIL_EVENTS) or (dialstatus in FAIL_NO_RULES_EVENTS))

    @classmethod
    def decode_fail_event(cls, ari_event_data):
        dialstatus = ari_event_data.get('dialstatus')
        if dialstatus != "NOANSWER":
            return dialstatus
        dialstring = ari_event_data.get('dialstring')
        pattern_timeout = r'^camp_\d+@omlacd$'
        if re.match(pattern_timeout, dialstring):
            return "TIMEOUT"
        return "NOANSWER"

    @classmethod
    def handle_fail_event(cls, ari_event_data, id_campaign, contact_id, phone_number):
        event = cls.decode_fail_event(ari_event_data)
        logger.debug(f'Campaign {id_campaign}: receiving {event} for contact {contact_id}')
        cls.set_contact_status(id_campaign, contact_id, event)
        if event in FAIL_EVENTS:
            cls.handle_incidence_rules(event, id_campaign, contact_id, phone_number)

    @classmethod
    def is_answer_event(cls, ari_event_data):
        dialstatus = ari_event_data.get('dialstatus')
        type_event = ari_event_data.get('type')
        return type_event == 'Dial' and dialstatus == 'ANSWER'

    @classmethod
    def get_contact_data(cls, ari_event_data):
        return ari_event_data['peer']['caller']['name'].split('_')

    @classmethod
    def is_answered_pstn(cls, ari_event_data):
        return ari_event_data['dialstring'].find('camp_') == -1

    @classmethod
    def is_answered_agent(cls, ari_event_data):
        return ari_event_data['dialstring'].find('camp_') >= 0

    @classmethod
    def set_contact_status(cls, id_campaign, contact_id, status, type_status=PHONE_TYPE):
        status_code = NAME_TO_STATUS[status]
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            if status_code == STATUS_ANSWERED_PSTN:
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET status_pstn = true, '
                    'history = array_append(history, %s)'
                    ' WHERE id_campaign = %s AND id_contact = %s;',
                    (str((status_code, type_status)), id_campaign, contact_id))
            else:
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET status = %s, '
                    'history = array_append(history, %s)'
                    ' WHERE id_campaign = %s AND id_contact = %s;',
                    (status_code, str((status_code, type_status)), id_campaign, contact_id))
            cls.connect_redis_dialer()
            cls.REDIS_DIALER_CONNECTION.rpush(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}:HISTORY', str((status_code, type_status)))
            cls.REDIS_DIALER_CONNECTION.hincrby(
                f'CAMP:{id_campaign}:COUNTER',
                status
            )

    @classmethod
    @job_handler_decorator
    def delete_campaign(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        logger.debug(f'Removing campaign with id = {id_campaign}')
        cls.connect_redis_dialer()
        cls.connect_redis_oml()
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM campaign WHERE id = %s;', (id_campaign,))
            cls.REDIS_DIALER_CONNECTION.delete(f'OML:CALLS:{id_campaign}:DIALER')
            # TODO: remove the remaining data in Redis
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({'type': 'DELETE',
                            'camp_id': id_campaign}))
        return b'Campaign was deleted'

    @classmethod
    def set_campaign_status(cls, id_campaign, new_status, cursor=None, sync_omnileads=False):
        if not sync_omnileads:
            if cursor is None:
                with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        'UPDATE campaign SET dialer_status = %s WHERE id = %s;',
                        (new_status, id_campaign))
            else:
                cursor.execute(
                    'UPDATE campaign SET dialer_status = %s WHERE id = %s;',
                    (new_status, id_campaign))
        else:
            with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
                with conn_dialer.transaction():
                    cursor_dialer = conn_dialer.cursor()
                    with psycopg.connect(cls.POSTGRES_OML_CONNECTION_STR) as conn_oml:
                        cursor_oml = conn_oml.cursor()
                        cursor_dialer.execute(
                            'UPDATE campaign SET dialer_status = %s WHERE id = %s;',
                            (new_status, id_campaign))
                        cursor_oml.execute(
                            'UPDATE ominicontacto_app_campana SET estado = %s WHERE id = %s;',
                            (new_status, id_campaign))
        cls.connect_redis_oml()
        cls.REDIS_OML_CONNECTION.publish(
            "OML:CHANNEL:DIALER",
            json.dumps({'type': 'STATUSCHANGE',
                        'camp_id': id_campaign,
                        'status': new_status,
                        'admin': AdminRender.render_status_change(
                            id_campaign, new_status, CAMPAIGN_STATUS_TO_NAME[new_status],
                            AVAILABLE_NEXT_STATUSES[new_status])}))

    @classmethod
    @job_handler_decorator
    def stop_campaign(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        logger.debug(f'Stopping campaign with id = {id_campaign}')
        cls.set_campaign_status(id_campaign, FINALIZED, sync_omnileads=sync_omnileads)
        return b'Campaign was finalized'

    @classmethod
    def update_prev_stats(cls, id_campaign):
        """Copy the current stats to the key 'COUNTER_PREV' so it can be used for comparison
        and send only the modified keys"""
        for key, value in cls.REDIS_DIALER_CONNECTION.hgetall(
                f'CAMP:{id_campaign}:COUNTER').items():
            cls.REDIS_DIALER_CONNECTION.hset(f'CAMP:{id_campaign}:COUNTER_PREV', key, value)

    @classmethod
    @job_handler_decorator
    def send_reports(cls, worker, job):
        cls.connect_redis_dialer()
        ari_event_data = cls.decode_payload(job.data)
        id_campaign, contact_id, phone_number = cls.get_contact_data(ari_event_data)
        previous_stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER_PREV') \
            or {}
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                """SELECT COUNT(*) FROM ONLY contact_in_campaign WHERE id_campaign = %s
                and (status = %s or status = %s);""",
                (id_campaign, STATUS_CREATED, STATUS_SELECTED_CALL))
            pending_for_call = cursor_dialer.fetchone()[0]
            cursor_dialer.execute("""SELECT final_status, COUNT(*) FROM ONLY contact_in_campaign
            WHERE id_campaign = %s and final_status <> %s GROUP BY final_status;""",
                                  (id_campaign, INITIAL))

            final_statuses = {
                PENDING_ATTEMPTS: 0,
                FINALIZED_NOCONTACT: 0,
                FINALIZED_SUCCESS: 0
            }
            for final_status_label, final_status_value in cursor_dialer.fetchall():
                final_statuses[final_status_label] = final_status_value

            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                FINAL_STATUS_TO_NAME[PENDING_ATTEMPTS],
                final_statuses[PENDING_ATTEMPTS]
            )
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                FINAL_STATUS_TO_NAME[FINALIZED_NOCONTACT],
                final_statuses[FINALIZED_NOCONTACT]
            )
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                FINAL_STATUS_TO_NAME[FINALIZED_SUCCESS],
                final_statuses[FINALIZED_SUCCESS]
            )
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                'PENDING_INITIAL_CONTACT_ATTEMPTS',  # pending to be contacted for the first time
                pending_for_call
            )
            stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER')
            logger.debug(f'Report for campaign {id_campaign}: {stats}')
            cls.connect_redis_oml()
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({
                    'type': 'EVENT',
                    'camp_id': id_campaign,
                    'data': ari_event_data
                }))
            stats_message = {
                'type': 'STATS',
                'camp_id': id_campaign,
            }
            # for Redis PUBSUB
            changed_stats = dict(set(stats.items()) - set(previous_stats.items()))
            changed_stats.update(stats_message)
            changed_stats.update({'admin': AdminRender.render_stats_inner(
                id_campaign, stats)})
            changed_stats_json = json.dumps(changed_stats)
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                changed_stats_json)
            cls.update_prev_stats(id_campaign)
            # for Postgres in Omnidialer
            stats_json = json.dumps(stats)
            cursor_dialer.execute(
                'UPDATE campaign SET statistics = %s WHERE id = %s;', (stats_json, id_campaign))
            return b'Success!'

    @classmethod
    def handle_disposition_option(cls, data):
        id_campaign = data['id_campaign']
        id_contact = data['id_contact']
        disposition_option = data['disposition_option']
        logger.debug(f'Adding disposition option {disposition_option} to contact {id_contact}'
                     f' in campaign {id_campaign}')
        cls.connect_redis_dialer()
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                """UPDATE contact_in_campaign as cc
                SET disposition_option = %s, history = array_append(history, %s)
                FROM contact AS co
                WHERE cc.id_campaign = %s AND cc.id_contact = %s AND co.id = cc.id_contact
                RETURNING co.phone;""",
                (disposition_option, str((disposition_option, DISPOSITION_TYPE)),
                 id_campaign, id_contact))
            phone_number = cursor_dialer.fetchone()[0]
            cls.REDIS_DIALER_CONNECTION.rpush(
                f'CONTACT:{id_contact}:CAMP:{id_campaign}:HISTORY',
                str((disposition_option, DISPOSITION_TYPE)))
            cls.connect_redis_dialer()
            incidence_rule = cls.get_incidence_rule_disposition(id_campaign, disposition_option)
            incidence_rule_applied = cls.apply_incidence_rule(
                cursor_dialer, incidence_rule, id_contact, id_campaign, disposition_option,
                DISPOSITION_TYPE, phone_number)
            # if the incidence rule was applied and the campaign is paused, reactivate the campaign
            if incidence_rule_applied:
                status = cls.get_campaign_status(id_campaign, cursor_dialer)
                if status == PAUSED:
                    cls.set_campaign_status(id_campaign, ACTIVE, cursor=cursor_dialer,
                                            sync_omnileads=True)
                    message = json.dumps({'id_campaign': id_campaign})
                    cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
            return b'Disposition for incidence rule was added!'

    @classmethod
    def handle_amd_option(cls, data):
        event = 'TERMINATED'
        id_campaign = data['id_campaign']
        id_contact = data['id_contact']
        phone_number = data['phone_number']
        logger.debug(f'Campaign {id_campaign}: receiving {event} for contact {id_contact}')
        cls.set_contact_status(id_campaign, id_contact, event)
        cls.handle_incidence_rules(event, id_campaign, id_contact, phone_number)
        return b'Disposition for AMD was handled'

    @classmethod
    @job_handler_decorator
    def add_incidence_rule_disposition(cls, worker, job):
        data = cls.decode_payload(job.data)
        disposition_option = data['disposition_option']
        if disposition_option == -2:
            return cls.handle_amd_option(data)
        return cls.handle_disposition_option(data)

    @classmethod
    @job_handler_decorator
    def create_incidence_rule(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        id_rule = data['id_rule']
        type_rule = data['type_rule']
        status_custom = data['status_custom']
        max_attempt = data['max_attempt']
        retry_later = data['retry_later']
        mode = data['mode']
        STATUS = 1
        logger.debug(f'Adding incidence rule to campaign {id_campaign}')
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            if type_rule == STATUS:
                status = data['status']
                cursor_dialer.execute(
                    """INSERT INTO incidence_rules (id, status, status_custom, max_attempt,
                    retry_later, in_mode, campaign_id) VALUES
                    (%s, %s, %s, %s, %s, %s, %s);""",
                    (id_rule, status, status_custom, max_attempt, retry_later, mode, id_campaign))
            else:
                disposition_option_id = data['disposition_option_id']
                cursor_dialer.execute(
                    """INSERT INTO incidence_rules_disposition (id, disposition_option_id,
                    max_attempt, retry_later, in_mode, campaign_id) VALUES
                    (%s, %s, %s, %s, %s, %s);""",
                    (id_rule, disposition_option_id, max_attempt, retry_later, mode, id_campaign))
            return b'Incidence rule was added'

    @classmethod
    @job_handler_decorator
    def delete_incidence_rule(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        id_rule = data['id_rule']
        type_rule = data['type_rule']
        type_rules = ['STATUS', 'DISPOSITION']
        type_rule_label = type_rules[type_rule - 1]
        STATUS = 1
        logger.debug(f'Removing incidence_rule {id_rule} of type {type_rule_label} '
                     f'in campaign with id = {id_campaign}')
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn:
            cursor = conn.cursor()
            if type_rule == STATUS:
                cursor.execute('DELETE FROM incidence_rules WHERE id = %s;', (id_rule,))
            else:
                cursor.execute('DELETE FROM incidence_rules_disposition WHERE id = %s;', (id_rule,))
            return b'Incidence rule was deleted'

    @classmethod
    @job_handler_decorator
    def update_incidence_rule(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        id_rule = data['id_rule']
        type_rule = data['type_rule']
        status_custom = data['status_custom']
        max_attempt = data['max_attempt']
        retry_later = data['retry_later']
        mode = data['mode']
        STATUS = 1
        logger.debug(f'Adding incidence rule to campaign {id_campaign}')
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            if type_rule == STATUS:
                status = data['status']
                cursor_dialer.execute(
                    """UPDATE incidence_rules SET status = %s, status_custom = %s,
                    max_attempt = %s, retry_later = %s, in_mode = %s, campaign_id = %s
                    WHERE id = %s;""",
                    (status, status_custom, max_attempt, retry_later, mode, id_campaign, id_rule))
            else:
                disposition_option_id = data['disposition_option_id']
                cursor_dialer.execute(
                    """UPDATE incidence_rules_disposition SET disposition_option_id = %s,
                    status_custom = %s, max_attempt = %s, retry_later = %s, in_mode = %s,
                    campaign_id = %s WHERE id = %s;""",
                    (disposition_option_id, max_attempt, retry_later, mode, id_campaign, id_rule))
            return b'Incidence rule was updated'

    @classmethod
    @job_handler_decorator
    def schedule_agenda(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        host = SCHEDULER_API_HOST
        uri = f'http://{host}/add-agenda/{id_campaign}'
        requests.post(uri, json=data)
        return b'Agenda was scheduled'

    @classmethod
    @job_handler_decorator
    def change_database(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        # 0- Pause the campaign
        cls.set_campaign_status(id_campaign, PAUSED, sync_omnileads=True)
        # 1- remove Redis related reports & contacts history
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.delete(f'CAMP:{id_campaign}:COUNTER')
        for key in cls.REDIS_DIALER_CONNECTION.scan_iter(
                match=f'CONTACT:*:CAMP:{id_campaign}', count=1000):
            AverageWorker.REDIS_DIALER_CONNECTION.delete(key)
        for key in cls.REDIS_DIALER_CONNECTION.scan_iter(
                match=f'CONTACT:*:CAMP:{id_campaign}:HISTORY', count=1000):
            AverageWorker.REDIS_DIALER_CONNECTION.delete(key)
        # 2- remove Postgres related reports & contacts history
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with psycopg.connect(cls.POSTGRES_OML_CONNECTION_STR) as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    cursor_dialer.execute(
                        'DELETE FROM contact_in_campaign WHERE id_campaign = %s', (id_campaign,))
                    cursor_dialer.execute(
                        'UPDATE campaign SET statistics = NULL WHERE id = %s', (id_campaign,))
                    # 3- bring the new contacts from OML
                    cls.copy_contacts_from_oml(cursor_dialer, cursor_oml, id_campaign)
        return b'Database was updated'

    @classmethod
    @job_handler_decorator
    def render_template(cls, worker, job):
        # Job dedicated to HTMX rendering
        data = cls.decode_payload(job.data)
        if data['type'] == 'init':
            logger.debug('HTMX related: getting the information of campaigns for the first time')
            with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute('SELECT id, name, dialer_status from campaign;')
                campaigns = cursor_dialer.fetchall()
                # transforming dialer_status value to a label
                campaigns = [(id_camp, name, CAMPAIGN_STATUS_TO_NAME[status],
                              AVAILABLE_NEXT_STATUSES[status])
                             for (id_camp, name, status) in campaigns]
                cursor_dialer.execute('SELECT is_active FROM system_control;')
                running = cursor_dialer.fetchone()[0]
                return AdminRender.render_init(campaigns, running)
        if data['type'] == 'stats':
            id_campaign = data['id_campaign']
            cls.connect_redis_dialer()
            stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER')
            return AdminRender.render_stats(id_campaign, stats)

    @classmethod
    def stop_dialer(cls):
        logger.debug('Stopping dialer')
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with psycopg.connect(cls.POSTGRES_OML_CONNECTION_STR) as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    cursor_dialer.execute(
                        'UPDATE system_control SET is_active = false, updated_at = now()'
                        ' WHERE id = true;')
                    logger.debug('Pausing all active campaigns')
                    cursor_dialer.execute(
                        'UPDATE campaign SET dialer_status = %s WHERE dialer_status = %s'
                        ' RETURNING id;',
                        (PAUSED, ACTIVE))
                    cursor_oml.execute(
                        'UPDATE ominicontacto_app_campana SET estado = %s WHERE estado = %s;',
                        (PAUSED, ACTIVE))
                    cls.connect_redis_oml()
                    cls.REDIS_OML_CONNECTION.publish(
                        "OML:CHANNEL:DIALER",
                        json.dumps({'type': 'PAUSE_BULK',
                                    'camp_id': 'all'})
                    )

    @classmethod
    def start_dialer(cls):
        logger.debug('Starting dialer')
        with psycopg.connect(cls.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'UPDATE system_control SET is_active = true, updated_at = now() WHERE id = true;')

    @classmethod
    def handle_dialer_action(cls, action):
        if action == 'start':
            cls.start_dialer()
        elif action == 'stop':
            cls.stop_dialer()
        else:
            cls.stop_dialer()
            cls.start_dialer()

    @classmethod
    @job_handler_decorator
    def manage_dialer(cls, worker, job):
        data = cls.decode_payload(job.data)
        action = data['action']
        cls.handle_dialer_action(action)
        cls.connect_redis_oml()
        cls.REDIS_OML_CONNECTION.publish(
            'OML:CHANNEL:DIALER',
            json.dumps({'type': 'DIALER_STATUS_CHANGE',
                        'action': action,
                        'camp_id': 'all'})
        )
        running = True
        if action == "stop":
            running = False
        return AdminRender.render_status_dialer(running)
