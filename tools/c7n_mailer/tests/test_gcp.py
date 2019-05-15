# Copyright 2019 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json
import unittest
import zlib
from common import logger, MAILER_CONFIG_1, GCP_MESSAGE, GCP_MESSAGES
from c7n_mailer.gcp.gcp_pubsub_processor import MailerGcpPubSubProcessor
from mock import MagicMock, patch


class GcpTest(unittest.TestCase):

    def setUp(self):
        self.compressed_message = MagicMock()
        self.compressed_message.content = base64.b64encode(
            zlib.compress(GCP_MESSAGE.encode('utf8')))
        self.loaded_message = json.loads(GCP_MESSAGE)

    @patch.object(MailerGcpPubSubProcessor, 'receive_messages')
    @patch.object(MailerGcpPubSubProcessor, 'process_message')
    @patch.object(MailerGcpPubSubProcessor, 'ack_messages')
    def test_process_pubsub_message_success(self, mock_ack, mock_process, mock_receive):
        mock_process.return_value = True
        mock_ack.return_value = True
        mock_receive.return_value = GCP_MESSAGES
        processor = MailerGcpPubSubProcessor(MAILER_CONFIG_1, logger)
        self.assertEqual(processor.receive_messages(), GCP_MESSAGES)
        mock_receive.assert_called()
        self.assertTrue(processor.process_message(self.compressed_message))
        processor.ack_messages('2019-05-13T18:31:17.926Z')
        mock_ack.assert_called_once_with('2019-05-13T18:31:17.926Z')

    @patch.object(MailerGcpPubSubProcessor, 'run')
    def test_run(self, mock_run):
        mock_run.return_value = None
        processor = MailerGcpPubSubProcessor(MAILER_CONFIG_1, logger)
        processor.run()
        self.assertEqual(1, mock_run.call_count)
        mock_run.assert_called()
