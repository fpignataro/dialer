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

    def _restore_env(self, key, value):
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


if __name__ == '__main__':
    unittest.main()