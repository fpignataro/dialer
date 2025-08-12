# -*- coding: utf-8 -*-

import datetime

import unittest

import uuid

from unittest.mock import MagicMock

from decimal import Decimal

import psycopg

import json

from datetime import timedelta
from gearman.job import GearmanJob
from gearman.worker import GearmanWorker

from handler.naive import (AverageWorker, ACTIVE, PAUSED, CREATED, FINALIZED, STATUS_SELECTED_CALL,
                           STATUS_CREATED, requests)


class MyTestSuite(unittest.TestCase):

    ORIGINAL_PSYCOPG_CONNECT = psycopg.connect

    def setUp(self):
        self.fetchmany_counter = 0
        self._create_campaign()

    def tearDown(self):
        self.clean_databases()

    @classmethod
    def encode_payload(cls, data):
        return bytes(json.dumps(data), encoding="UTF8")

    def clean_databases(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.flushdb()
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('DELETE FROM campaign;')
            cursor_dialer.execute('DELETE FROM contact;')
            cursor_dialer.execute('DELETE FROM incidence_rules;')
            cursor_dialer.execute('DELETE FROM incidence_rules_disposition;')
            cursor_dialer.execute('DELETE FROM contact_in_campaign;')
            cursor_dialer.execute('UPDATE system_control SET is_active = true;')
            cursor_dialer.execute('DELETE FROM jobs;')
        AverageWorker.REDIS_DIALER_CONNECTION.close()

    def mocked_get_contacts_campaign(self, cursor, size):
        if self.fetchmany_counter == 0:
            self.fetchmany_counter += 1
            return [(1, '6093017590',
                     '["Amanda Jenkins", "Gregory Henson", "7147034", "4067530816", "5273724517"]',
                     True),
                    (2, '5143016455',
                     '["Ashley Barrett", "Edward Townsend", "8718745", "5618936401", "1075763364"]',
                     True)]
        return []

    def mocked_get_contacts_campaign_no_phone(self, cursor, size):
        if self.fetchmany_counter == 0:
            self.fetchmany_counter += 1
            return [(1, '',
                     '["Amanda Jenkins", "Gregory Henson", "7147034", "4067530816", "5273724517"]',
                     True),
                    (2, '5143016455',
                     '["Ashley Barrett", "Edward Townsend", "8718745", "5618936401", "1075763364"]',
                     True)]
        return []

    def mocked_psycopg_connect(self, connection_str):
        if connection_str == AverageWorker.POSTGRES_OML_CONNECTION_STR:
            return MagicMock()
        return self.ORIGINAL_PSYCOPG_CONNECT(connection_str)

    def _create_campaign(self):
        # mocking Postgres connection to OML
        psycopg.connect = MagicMock(side_effect=self.mocked_psycopg_connect)
        # mocking get_campaign_data
        self.campaign_id_data = (
            4, 2, 'test_dialer_01', datetime.date(2024, 8, 21),
            datetime.datetime.now().date(), 2, 10, 'rrmemory', 10, False, Decimal('1.0'), 1, False,
            True, False, False, False, False, False, datetime.time(15, 51), datetime.time(15, 51),
            [1, 3, 4], 1,
            '"{\\"prim_fila_enc\\": false, \\"cant_col\\": 6, \\"nombres_de_columnas\\": '
            '[\\"telefono\\", \\"nombre\\", \\"apellido\\", \\"dni\\", \\"telefono2\\", '
            '\\"telefono3\\"], \\"cols_telefono\\": [0, 4, 5]}"', '0')
        self.incidence_rules_data = [(1, 1, 'busy', 4, 20, 1, 4), (2, 4, 'congestion', 3, 40, 1, 4)]
        self.incidence_rules_disposition_data = [(1, 7, 3, 17, 1, 4), (2, 8, 5, 7, 2, 4)]
        campaign_mocked_data = (self.campaign_id_data, self.incidence_rules_data,
                                self.incidence_rules_disposition_data)
        AverageWorker.get_campaign_data = MagicMock(
            return_value=campaign_mocked_data)
        AverageWorker.get_contacts_campaign = MagicMock(
            side_effect=self.mocked_get_contacts_campaign)
        self.worker = GearmanWorker()
        job = GearmanJob(None, None, b'create-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4", "contact_strategy": [1, 3, 4], "prefix": ""}')
        AverageWorker.create_campaign(self.worker, job)

    def test_clean_broken_selected_contacts_starting_campaing(self):
        # mark one contact to SELECT_CALL status
        # run start_campaign
        # ensure the contact has now CREATED status
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('UPDATE contact_in_campaign SET status = %s WHERE id_contact = %s'
                                  ' AND id_campaign = %s;',
                                  (STATUS_SELECTED_CALL, 1, 4))
        job = GearmanJob(None, None, b'start-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4", "sync_omnileads": "false"}')
        AverageWorker.start_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) FROM contact_in_campaign WHERE id_campaign = 4'
                                  ' AND status = %s;', (STATUS_CREATED,))
        self.assertEqual(cursor_dialer.fetchone()[0], 2)

    def test_clean_broken_selected_contacts_resuming_campaing(self):
        # mark one contact to SELECT_CALL status
        # run resume_campaign
        # ensure the contact has now CREATED status
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('UPDATE contact_in_campaign SET status = %s WHERE id_contact = %s'
                                  ' AND id_campaign = %s;',
                                  (STATUS_SELECTED_CALL, 1, 4))
        job = GearmanJob(None, None, b'resume-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4", "sync_omnileads": "false"}')
        AverageWorker.resume_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) FROM contact_in_campaign WHERE id_campaign = 4'
                                  ' AND status = %s;', (STATUS_CREATED,))
        self.assertEqual(cursor_dialer.fetchone()[0], 2)

    def gen_fail_event(self, event):
        return {'type': 'Dial',
                'timestamp': '2025-04-15T11:21:29.168-0300',
                'dialstatus': event,
                'forward': '',
                'dialstring': '123456720@pstn_gateway',
                'peer': {'id': '1744726885.6',
                         'name': 'PJSIP/pstn_gateway-00000006',
                         'state': 'Down',
                         'protocol_id': '108c4adb-09f6-4271-94bc-4d2a9cf468b4',
                         'caller': {'name': '4_1_6093017590', 'number': ''},
                         'connected': {'name': '1_1_6093017590', 'number': ''},
                         'accountcode': '',
                         'dialplan': {'context': 'from-omlacd',
                                      'exten': 's',
                                      'priority': 1,
                                      'app_name': 'AppDial2',
                                      'app_data': '(Outgoing Line)'},
                         'creationtime': '2025-04-15T11:21:25.125-0300',
                         'language': 'en'},
                'asterisk_id': '26:ce:a5:36:bc:0a', 'application': 'call_manager_dialer'}

    def test_chanunavailable_events(self):
        # make sure if an chanunavailable event came to process event a call won't be scheduled
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        chanunavail_event = self.gen_fail_event('CHANUNAVAIL')
        job = GearmanJob(None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps(chanunavail_event), encoding="UTF8"))
        AverageWorker.process_event(self.worker, job)
        self.assertNotEqual(AverageWorker.GM_CLIENT.submit_job.call_args_list[0][0][0],
                            'schedule-agenda')

    def test_incidence_rules(self):
        # make sure if an event came to process event and there is an incidence rule attached to it
        # it will schedule a call if the contact has still a valid number of attempts
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        busy_event = self.gen_fail_event('BUSY')
        job = GearmanJob(None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps(busy_event), encoding="UTF8"))
        AverageWorker.process_event(self.worker, job)
        # check that a job was submitted to 'schedule-contact'
        self.assertEqual(AverageWorker.GM_CLIENT.submit_job.call_args_list[0][0][0],
                         'schedule-agenda')

    def test_incidence_rules_disposition(self):
        # make sure if a disposition came to the disposition endpoint and there is an incidence rule
        # disposition attached to it  will schedule a call if the contact has still a valid number
        # of attempts
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        job = GearmanJob(None, None, b'add-incidence-rule-disposition',
                         bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps({"id_campaign": "4", "disposition_option": 7,
                                           "id_contact": 1}), encoding="UTF8"))
        AverageWorker.add_incidence_rule_disposition(self.worker, job)
        # check that a job was submitted to 'schedule-contact'
        self.assertEqual(AverageWorker.GM_CLIENT.submit_job.call_args_list[0][0][0],
                         'schedule-agenda')

    def test_call_is_tagged_as_aborted_if_campaign_not_active(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("UPDATE campaign SET dialer_status = %s WHERE id = 4;",
                                  (PAUSED,))
        job = GearmanJob(None, None, b'process-contact', bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps({"contact": [1, 4, 6093017590], "id_campaign": 4}),
                               encoding="utf8"))
        AverageWorker.process_contact(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("SELECT schedule_aborted FROM contact_in_campaign"
                                  " WHERE id_contact = 1;")
            self.assertEqual(cursor_dialer.fetchone()[0], True)

    def test_campaign_is_paused_if_expired(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("UPDATE campaign SET"
                                  " start_date = CURRENT_DATE - INTERVAL '2 day',"
                                  "end_date = CURRENT_DATE - INTERVAL '1 day'"
                                  " WHERE id = 4;")
        job = GearmanJob(None, None, b'process-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.process_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("SELECT dialer_status from campaign WHERE id = 4;")
            self.assertEqual(cursor_dialer.fetchone()[0], PAUSED)

    def test_campaign_is_forbidden_to_start_if_dialer_stopped(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("UPDATE system_control SET is_active = false;")

        job = GearmanJob(None, None, b'start-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.start_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("SELECT dialer_status from campaign WHERE id = 4;")
            self.assertEqual(cursor_dialer.fetchone()[0], CREATED)

    def test_campaign_is_forbidden_to_resume_if_dialer_stopped(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("UPDATE system_control SET is_active = false;")
            cursor_dialer.execute("UPDATE campaign SET dialer_status = %s WHERE id = 4;", (PAUSED,))

        job = GearmanJob(None, None, b'resume-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.resume_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("SELECT dialer_status from campaign WHERE id = 4;")
            self.assertEqual(cursor_dialer.fetchone()[0], PAUSED)

    def test_campaign_is_paused_if_active_after_dialer_stop(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("UPDATE campaign SET dialer_status = %s WHERE id = 4;", (ACTIVE,))

        job = GearmanJob(None, None, b'manage-dialer', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"action": "stop"}')
        AverageWorker.manage_dialer(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("SELECT dialer_status from campaign WHERE id = 4;")
            self.assertEqual(cursor_dialer.fetchone()[0], PAUSED)

    def test_campaign_is_deleted_correctly(self):
        job = GearmanJob(None, None, b'delete-campaign',
                         bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.delete_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT * from campaign;')
            self.assertEqual(cursor_dialer.fetchall(), [])

    def test_job_entry_is_removed_if_ok(self):
        job = GearmanJob(None, None, b'add-incidence-rule-disposition',
                         bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps({"id_campaign": "4", "disposition_option": 7,
                                           "id_contact": 1}), encoding="UTF8"))
        AverageWorker.add_incidence_rule_disposition(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT * from jobs;')
            self.assertEqual(cursor_dialer.fetchall(), [])

    def test_job_entry_is_saved_if_error(self):
        AverageWorker.clean_selected_contacts = MagicMock(side_effect=ValueError)
        payload = {
            'id_campaign': 4,
            'sync_omnileads': False
        }
        payload_bytes = self.encode_payload(payload)
        job = GearmanJob(None, None, b'start-campaign',
                         bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
        with self.assertRaises(ValueError):
            AverageWorker.start_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT * from jobs;')
            self.assertEqual(len(cursor_dialer.fetchall()), 1)

    def test_contacts_without_phone_not_imported(self):
        self.fetchmany_counter = 0
        AverageWorker.get_contacts_campaign = MagicMock(
            side_effect=self.mocked_get_contacts_campaign_no_phone)
        job = GearmanJob(None, None, b'change-database',
                         bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.change_database(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) from contact_in_campaign WHERE id_campaign = 4;')
            self.assertEqual(cursor_dialer.fetchone()[0], 1)

    def test_campaign_max_available_channels_cache_invalidation(self):
        camp_id = 4
        self.assertEqual(AverageWorker.get_campaign_max_available_channels(camp_id), 1)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('UPDATE campaign SET max_channels = 3 WHERE id = %s;', (camp_id,))
        self.assertEqual(AverageWorker.get_campaign_max_available_channels(camp_id), 1)
        new_campaign_id_data = self.campaign_id_data[:11] + (3,) + self.campaign_id_data[12:]
        campaign_mocked_data = (new_campaign_id_data, self.incidence_rules_data,
                                self.incidence_rules_disposition_data)
        AverageWorker.get_campaign_data = MagicMock(return_value=campaign_mocked_data)
        job = GearmanJob(None, None, b'edit-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps({'id_campaign': '4', 'contact_strategy': [1, 3, 4]}),
                               encoding='UTF8'))
        AverageWorker.edit_campaign(self.worker, job)
        self.assertEqual(AverageWorker.get_campaign_max_available_channels(camp_id), 3)

    def test_amd_event_apply_incidence_rules(self):
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                """INSERT INTO incidence_rules (id, status, status_custom, max_attempt,
                retry_later, in_mode, campaign_id) VALUES
                (%s, %s, %s, %s, %s, %s, %s);""",
                (3, 2, 'terminated', 1, 7, 1, 4))
        # testing endpoint add disposition for incidence rule
        job = GearmanJob(
            None, None, b'add-incidence-rule-disposition',
            bytes(str(uuid.uuid4()), encoding='utf8'),
            b'{"id_campaign": "4", "disposition_option": -2, "id_contact": 1, '
            b'"phone_number": "12343556"}')

        # a call is scheduled for the first time
        AverageWorker.add_incidence_rule_disposition(self.worker, job)
        self.assertTrue(AverageWorker.GM_CLIENT.submit_job.called)
        AverageWorker.GM_CLIENT.submit_job.reset_mock()

        # a call is not scheduled for the second time because the incidence rule counter was
        # consumed
        AverageWorker.add_incidence_rule_disposition(self.worker, job)
        self.assertFalse(AverageWorker.GM_CLIENT.submit_job.called)
        AverageWorker.GM_CLIENT.submit_job.reset_mock()

    def test_suspend_campaign_schedules_call_next_day(self):
        current_date = datetime.datetime.now().date()
        extra_info = (False,                            # failed day of week match
                      0,                                # Sunday
                      False,                            # hour match
                      current_date,                     # current_date,
                      17,                               # hour,
                      9,                                # minute,
                      # campaign_info
                      (datetime.time(17, 10), datetime.time(17, 17), True, True, True, True, True,
                       True, True),
                      )
        AverageWorker.opening_hours_match = MagicMock(return_value=(False, extra_info))
        requests.post = MagicMock()
        AverageWorker.set_campaign_status(4, ACTIVE)
        job = GearmanJob(None, None, b'process-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.process_campaign(self.worker, job)
        self.assertTrue(requests.post.called)
        self.assertEqual(requests.post.call_args[0][0],
                         'http://scheduler-api/add-process-campaign/4')
        expected_date = current_date + timedelta(days=1)
        expected_datetime = datetime.datetime.combine(expected_date, datetime.time(17, 10))
        self.assertEqual(requests.post.call_args[1]['json']['datetime_start'],
                         expected_datetime.strftime('%d/%m/%y %H:%M:%S'))

    def test_suspend_campaign_schedules_same_day(self):
        current_date = datetime.datetime.now().date()
        extra_info = (True,                             # failed day of week match
                      0,                                # Sunday
                      False,                            # hour match
                      current_date,                     # current_date,
                      17,                               # hour,
                      9,                                # minute,
                      # campaign_info
                      (datetime.time(17, 10), datetime.time(17, 17), True, True, True, True, True,
                       True, True),
                      )
        AverageWorker.opening_hours_match = MagicMock(return_value=(False, extra_info))
        requests.post = MagicMock()
        AverageWorker.set_campaign_status(4, ACTIVE)
        job = GearmanJob(None, None, b'process-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.process_campaign(self.worker, job)
        self.assertTrue(requests.post.called)
        self.assertEqual(requests.post.call_args[0][0],
                         'http://scheduler-api/add-process-campaign/4')
        expected_datetime = datetime.datetime.combine(current_date, datetime.time(17, 10))
        self.assertEqual(requests.post.call_args[1]['json']['datetime_start'],
                         expected_datetime.strftime('%d/%m/%y %H:%M:%S'))

    def test_handle_campaign_general(self):
        process_campaign_cm = AverageWorker.process_campaign
        AverageWorker.process_campaign = MagicMock()
        # check campaign entry creation and related tables too
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY campaign;')
            self.assertEqual(cursor_dialer.fetchone()[0], 1)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY incidence_rules;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY incidence_rules_disposition;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY contact_in_campaign;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY contact;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)

        # let's edit the campaign now
        job = GearmanJob(None, None, b'edit-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4", "contact_strategy": [1, 4]}')
        campaign_id_data = self.campaign_id_data[:-4] + ([1, 4],) + self.campaign_id_data[-3:]
        campaign_mocked_data = (campaign_id_data, self.incidence_rules_data,
                                self.incidence_rules_disposition_data)
        AverageWorker.get_campaign_data = MagicMock(
            return_value=campaign_mocked_data)
        AverageWorker.edit_campaign(self.worker, job)
        # let's add an incidence rule
        job = GearmanJob(
            None, None, b'create-incidence-rule', bytes(str(uuid.uuid4()), encoding='utf8'),
            b'{"id_campaign": 4, "id_rule": 3, "status": 3, "status_custom":"no answer", '
            b'"max_attempt": 5, "retry_later": 5, "mode": 1, "type_rule": 1}')
        AverageWorker.create_incidence_rule(self.worker, job)

        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            AverageWorker.GM_CLIENT.submit_job = MagicMock()
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY incidence_rules;')
            self.assertEqual(cursor_dialer.fetchone()[0], 3)
            cursor_dialer.execute('SELECT contact_strategy FROM ONLY campaign;')
            self.assertEqual(cursor_dialer.fetchone()[0], [1, 4])
            # now just edit the incidence rule
            job = GearmanJob(
                None, None, b'update-incidence-rule', bytes(str(uuid.uuid4()), encoding='utf8'),
                b'{"id_campaign": 4, "id_rule": 3, "status": 3, "status_custom":"no answer", '
                b'"max_attempt": 7, "retry_later": 5, "mode": 1, "type_rule": 1}')
            AverageWorker.update_incidence_rule(self.worker, job)
            id_rule = 3
            cursor_dialer.execute(
                'SELECT max_attempt FROM ONLY incidence_rules WHERE id = %s;',
                (id_rule,))
            max_attempt_value = cursor_dialer.fetchone()[0]
            self.assertEqual(max_attempt_value, 7)
            # let's remove an incidence rule
            job = GearmanJob(
                None, None, b'delete-incidence-rule', bytes(str(uuid.uuid4()), encoding='utf8'),
                b'{"id_campaign": 4, "id_rule": 3, "type_rule": 1}')
            AverageWorker.delete_incidence_rule(self.worker, job)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY incidence_rules;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)
            id_campaign = campaign_id_data[0]
            # testing start-campaign
            AverageWorker.process_campaign = MagicMock()
            payload = {
                'id_campaign': 4,
                'sync_omnileads': False
            }
            payload_bytes = self.encode_payload(payload)
            job = GearmanJob(None, None, b'start-campaign',
                             bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
            AverageWorker.start_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, ACTIVE)

            # testing pause-campaign
            job = GearmanJob(None, None, b'pause-campaign',
                             bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
            AverageWorker.pause_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, PAUSED)

            # testing resume-campaign
            job = GearmanJob(None, None, b'resume-campaign',
                             bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
            AverageWorker.resume_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, ACTIVE)

            # testing endpoint add disposition for incidence rule
            job = GearmanJob(
                None, None, b'add-incidence-rule', bytes(str(uuid.uuid4()), encoding='utf8'),
                b'{"id_campaign": "4", "disposition_option": 8, "id_contact": 1}')
            for i in range(5):
                AverageWorker.add_incidence_rule_disposition(self.worker, job)
                self.assertTrue(AverageWorker.GM_CLIENT.submit_job.called)
                AverageWorker.GM_CLIENT.submit_job.reset_mock()
            AverageWorker.add_incidence_rule_disposition(self.worker, job)
            self.assertFalse(AverageWorker.GM_CLIENT.submit_job.called)
            AverageWorker.GM_CLIENT.submit_job.reset_mock()

            # testing stop-campaign
            job = GearmanJob(None, None, b'stop-campaign',
                             bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
            AverageWorker.stop_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, FINALIZED)

            AverageWorker.process_campaign = process_campaign_cm


if __name__ == '__main__':
    unittest.main()
