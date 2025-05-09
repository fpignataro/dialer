# -*- coding: utf-8 -*-

import datetime

import unittest

from unittest.mock import MagicMock

from decimal import Decimal

import psycopg

import json

from gearman.job import GearmanJob
from gearman.worker import GearmanWorker

from handler.naive import (AverageWorker, ACTIVE, PAUSED, CREATED, FINALIZED, STATUS_SELECTED_CALL,
                           STATUS_CREATED)


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
        AverageWorker.REDIS_DIALER_CONNECTION.close()

    def mocked_psycopg_fetchmany(self, cursor, size):
        if self.fetchmany_counter == 0:
            self.fetchmany_counter += 1
            return [(1, '6093017590',
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
        AverageWorker.get_contacts_campaign = MagicMock(side_effect=self.mocked_psycopg_fetchmany)
        self.worker = GearmanWorker()
        job = GearmanJob(None, None, None, None,
                         b'{"id_campaign": "4", "contact_strategy": [1, 3, 4]}')
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
        job = GearmanJob(None, None, None, None,
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
        job = GearmanJob(None, None, None, None,
                         b'{"id_campaign": "4", "sync_omnileads": "false"}')
        AverageWorker.resume_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) FROM contact_in_campaign WHERE id_campaign = 4'
                                  ' AND status = %s;', (STATUS_CREATED,))
        self.assertEqual(cursor_dialer.fetchone()[0], 2)

    def test_incidence_rules(self):
        # make sure if an event came to process event and there is an incidence rule attached to it
        # it will schedule a call if the contact has still a valid number of attempts
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        busy_event = {'type': 'Dial',
                      'timestamp': '2025-04-15T11:21:29.168-0300',
                      'dialstatus': 'BUSY',
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
        job = GearmanJob(None, None, None, None,
                         bytes(json.dumps(busy_event), encoding="UTF8"))
        AverageWorker.process_event(self.worker, job)
        # check that a job was submitted to 'schedule-contact'
        self.assertEqual(AverageWorker.GM_CLIENT.submit_job.call_args_list[0][0][0],
                         'schedule-contact')

    def test_incidence_rules_disposition(self):
        # make sure if a disposition came to the disposition endpoint and there is an incidence rule
        # disposition attached to it  will schedule a call if the contact has still a valid number
        # of attempts
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        job = GearmanJob(None, None, None, None,
                         bytes(json.dumps({"id_campaign": "4", "disposition_option": 7,
                                           "id_contact": 1}), encoding="UTF8"))
        AverageWorker.add_incidence_rule_disposition(self.worker, job)
        # check that a job was submitted to 'schedule-contact'
        self.assertEqual(AverageWorker.GM_CLIENT.submit_job.call_args_list[0][0][0],
                         'schedule-contact')

    def test_call_is_tagged_as_aborted_if_campaign_not_active(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("UPDATE campaign SET dialer_status = %s WHERE id = 4;",
                                  (PAUSED,))
        job = GearmanJob(None, None, None, None,
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
        job = GearmanJob(None, None, None, None,
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

        job = GearmanJob(None, None, None, None,
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

        job = GearmanJob(None, None, None, None,
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

        job = GearmanJob(None, None, None, None,
                         b'{"action": "stop"}')
        AverageWorker.manage_dialer(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("SELECT dialer_status from campaign WHERE id = 4;")
            self.assertEqual(cursor_dialer.fetchone()[0], PAUSED)

    def test_handle_campaign_general(self):
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
        job = GearmanJob(None, None, None, None,
                         b'{"id_campaign": "4", "contact_strategy": [1, 4]}')
        campaign_id_data = self.campaign_id_data[:-4] + ([1, 4],) + self.campaign_id_data[-3:]
        campaign_mocked_data = (campaign_id_data, self.incidence_rules_data,
                                self.incidence_rules_disposition_data)
        AverageWorker.get_campaign_data = MagicMock(
            return_value=campaign_mocked_data)
        AverageWorker.edit_campaign(self.worker, job)
        # let's add an incidence rule
        job = GearmanJob(
            None, None, None, None,
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
                None, None, None, None,
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
                None, None, None, None,
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
            job = GearmanJob(None, None, None, None, payload_bytes)
            AverageWorker.start_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, ACTIVE)

            # testing pause-campaign
            job = GearmanJob(None, None, None, None, payload_bytes)
            AverageWorker.pause_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, PAUSED)

            # testing resume-campaign
            job = GearmanJob(None, None, None, None, payload_bytes)
            AverageWorker.resume_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, ACTIVE)

            # testing endpoint add disposition for incidence rule
            job = GearmanJob(
                None, None, None, None,
                b'{"id_campaign": "4", "disposition_option": 8, "id_contact": 1}')
            for i in range(5):
                AverageWorker.add_incidence_rule_disposition(self.worker, job)
                self.assertTrue(AverageWorker.GM_CLIENT.submit_job.called)
                AverageWorker.GM_CLIENT.submit_job.reset_mock()
            AverageWorker.add_incidence_rule_disposition(self.worker, job)
            self.assertFalse(AverageWorker.GM_CLIENT.submit_job.called)
            AverageWorker.GM_CLIENT.submit_job.reset_mock()

            # testing stop-campaign
            job = GearmanJob(None, None, None, None, payload_bytes)
            AverageWorker.stop_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, FINALIZED)


if __name__ == '__main__':
    unittest.main()
