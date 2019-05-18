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

import json
import unittest
from common import logger, MAILER_CONFIG_1, GCP_MESSAGE, GCP_MESSAGES
from c7n_mailer.gcp.gcp_pubsub_processor import MailerGcpPubSubProcessor
from c7n_mailer.email_delivery import EmailDelivery
from mock import patch


class GcpTest(unittest.TestCase):

    def setUp(self):
        self.compressed_message = GCP_MESSAGE
        self.loaded_message = json.loads(GCP_MESSAGE)

    @patch.object(EmailDelivery, 'send_c7n_email')
    def test_process_message(self, mock_email):
        mock_email.return_value = True
        processor = MailerGcpPubSubProcessor(MAILER_CONFIG_1, logger)
        self.assertIsNone(processor.process_message(GCP_MESSAGES['receivedMessages'][0]))

    @patch.object(MailerGcpPubSubProcessor, 'ack_messages')
    @patch.object(MailerGcpPubSubProcessor, 'receive_messages')
    def test_run(self, mock_receive):
        mock_receive.return_value = []
        processor = MailerGcpPubSubProcessor(MAILER_CONFIG_1, logger)
        processor.run()
