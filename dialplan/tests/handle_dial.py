import sys
from pathlib import Path
import unittest
from unittest.mock import MagicMock

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

from app import CallManager  # noqa: E402


class HandleDialTestCase(unittest.TestCase):

    def test_handle_dial_with_missing_call_data_does_not_publish(self):
        call_manager = CallManager.__new__(CallManager)
        call_manager.redis_client = MagicMock()
        call_manager.pstn_channel_ids = set()
        call_manager.agent_to_pstn = {}
        call_manager.channel_dialstatus = {}
        call_manager.pstngw_hostname = "example.com"
        call_manager.publish_message_if_needed = MagicMock()
        call_manager.parse_caller_name = MagicMock(return_value=(None, None, None))

        event = {
            'dialstatus': 'ANSWER',
            'dialstring': '123@pstn_gateway',
            'channel': {}
        }

        call_manager.handle_dial(event)

        call_manager.publish_message_if_needed.assert_not_called()


if __name__ == "__main__":
    unittest.main()
