# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import mock

from google.adk.tools.pubsub import client
from google.cloud import pubsub_v1
from google.oauth2.credentials import Credentials


@mock.patch("google.cloud.pubsub_v1.PublisherClient")
def test_get_publisher_client(mock_publisher_client):
  """Test get_publisher_client factory."""
  mock_creds = mock.Mock(spec=Credentials)
  client.get_publisher_client(credentials=mock_creds)

  mock_publisher_client.assert_called_once()
  _, kwargs = mock_publisher_client.call_args
  assert kwargs["credentials"] == mock_creds
  assert "client_info" in kwargs

  assert "client_info" in kwargs


@mock.patch("google.cloud.pubsub_v1.SubscriberClient")
def test_get_subscriber_client(mock_subscriber_client):
  """Test get_subscriber_client factory."""
  mock_creds = mock.Mock(spec=Credentials)
  client.get_subscriber_client(credentials=mock_creds)

  mock_subscriber_client.assert_called_once()
  _, kwargs = mock_subscriber_client.call_args
  assert kwargs["credentials"] == mock_creds
  assert "client_info" in kwargs
