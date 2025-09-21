import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

SRC_DIR = Path(__file__).resolve().parents[1] / 'src'
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from app import CallManager


class CallManagerDialStatusTest(unittest.TestCase):

    def _create_manager(self):
        manager = CallManager.__new__(CallManager)
        manager.ari = MagicMock()
        manager.calls = {}
        manager.ws = None
        manager.shutting_down = False
        manager.redis_client = MagicMock()
        manager.pstn_channel_ids = set()
        manager.agent_to_pstn = {}
        manager.channel_dialstatus = {}
        manager.gearman_task = 'call_log_processor'
        manager.gearman_hostport = 'localhost:4730'
        manager.gearman_client = MagicMock()
        manager.pstngw_hostname = 'pstn.example'
        manager.publish_message_if_needed = MagicMock()
        return manager

    @patch('dialplan.src.app.gearman.GearmanClient')
    @patch('dialplan.src.app.redis.Redis')
    @patch('dialplan.src.app.ARI')
    def test_channel_destroyed_uses_persisted_dialstatus(
        self,
        mock_ari,
        mock_redis,
        mock_gearman,
    ):
        mock_ari.return_value = MagicMock()

        redis_instance = MagicMock()
        redis_instance.exists.return_value = True
        mock_redis.return_value = redis_instance

        mock_gearman.return_value = MagicMock()

        original_hostname = os.environ.get('PSTNGW_HOSTNAME')
        os.environ['PSTNGW_HOSTNAME'] = 'pstn.example'
        self.addCleanup(self._restore_env, 'PSTNGW_HOSTNAME', original_hostname)

        manager = CallManager()
        manager.publish_message_if_needed = MagicMock()

        dial_event_initial = {
            'type': 'Dial',
            'dialstatus': '',
            'dialstring': '5551234@pstn_gateway',
            'peer': {
                'id': 'PSTN123',
                'caller': {'name': '1_2_5551234'},
            },
            'channel': {'id': 'LOCAL999'},
        }
        manager.handle_dial(dial_event_initial)

        dial_event_result = {
            'type': 'Dial',
            'dialstatus': 'NOANSWER',
            'dialstring': '5551234@pstn_gateway',
            'peer': {
                'id': 'PSTN123',
                'caller': {'name': '1_2_5551234'},
            },
            'channel': {'id': 'LOCAL999'},
        }
        manager.handle_dial(dial_event_result)

        self.assertEqual(manager.channel_dialstatus.get('PSTN123'), 'NOANSWER')

        manager.publish_message_if_needed.reset_mock()

        destroy_event = {
            'type': 'ChannelDestroyed',
            'channel': {
                'id': 'PSTN123',
                'caller': {'name': '1_2_5551234'},
            },
        }

        manager.handle_channel_destroyed(destroy_event)

        expected_call_data = {
            'id_camp': '1',
            'id_customer': '2',
            'tel_customer': '5551234',
            'channel_id': 'PSTN123',
            'dialstatus': 'NOANSWER',
        }

        manager.publish_message_if_needed.assert_called_once_with(
            expected_call_data,
            'NOANSWER',
        )

        self.assertNotIn('PSTN123', manager.channel_dialstatus)
        self.assertNotIn('PSTN123', manager.pstn_channel_ids)

    def test_agent_channel_destroyed_removes_mapping(self):
        manager = self._create_manager()
        manager.agent_to_pstn['AGENT123'] = 'PSTN789'
        manager.channel_dialstatus['PSTN789'] = 'BUSY'

        event = {
            'type': 'ChannelDestroyed',
            'channel': {
                'id': 'AGENT123',
                'caller': {'name': '1_2_5551234'},
            },
        }

        manager.handle_channel_destroyed(event)

        self.assertNotIn('AGENT123', manager.agent_to_pstn)
        self.assertEqual('BUSY', manager.channel_dialstatus.get('PSTN789'))

    def test_pstn_channel_destroyed_removes_agent_mappings(self):
        manager = self._create_manager()
        manager.agent_to_pstn['AGENT321'] = 'PSTN654'
        manager.pstn_channel_ids.add('PSTN654')
        manager.channel_dialstatus['PSTN654'] = 'NOANSWER'

        event = {
            'type': 'ChannelDestroyed',
            'channel': {
                'id': 'PSTN654',
                'caller': {'name': '1_2_5559876'},
            },
        }

        manager.handle_channel_destroyed(event)

        self.assertNotIn('AGENT321', manager.agent_to_pstn)
        self.assertIsNone(manager.agent_to_pstn.get('AGENT321'))
        self.assertEqual('NOANSWER', manager.channel_dialstatus.get('AGENT321'))
        self.assertNotIn('PSTN654', manager.channel_dialstatus)

    def test_channel_hangup_request_cleans_agent_to_pstn(self):
        manager = self._create_manager()
        manager.agent_to_pstn['AGENT555'] = 'PSTN777'
        manager.channel_dialstatus['PSTN777'] = 'BUSY'
        manager.calls = {
            'CALL1': {
                'channel_id': 'LOCAL111',
                'channel_id_pstn': 'PSTN777',
                'omlacd_channel_id': 'AGENT555',
                'bridge_id': 'BR42',
            }
        }

        event = {
            'type': 'ChannelHangupRequest',
            'channel': {'id': 'PSTN777'},
            'cause': 16,
        }

        manager.handle_channel_hangup_request(event)

        self.assertNotIn('AGENT555', manager.agent_to_pstn)
        self.assertNotIn('PSTN777', manager.agent_to_pstn)
        self.assertEqual('BUSY', manager.channel_dialstatus.get('AGENT555'))

    def _restore_env(self, key, value):
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


if __name__ == '__main__':
    unittest.main()