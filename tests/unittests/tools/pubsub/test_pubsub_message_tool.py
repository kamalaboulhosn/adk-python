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

from __future__ import annotations

import os
from unittest import mock

from google.adk.tools.pubsub import client as pubsub_client_lib
from google.adk.tools.pubsub import message_tool
from google.adk.tools.pubsub.config import PubSubToolConfig
from google.cloud import pubsub_v1
from google.oauth2.credentials import Credentials


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(pubsub_v1.PublisherClient, "publish", autospec=True)
@mock.patch.object(pubsub_client_lib, "get_publisher_client", autospec=True)
def test_publish_message(mock_get_publisher_client, mock_publish):
  """Test publish_message tool invocation."""
  topic_name = "projects/my_project_id/topics/my_topic"
  message = "Hello World"
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id="my_project_id")

  mock_publisher_client = mock.create_autospec(
      pubsub_v1.PublisherClient, instance=True
  )
  mock_get_publisher_client.return_value = mock_publisher_client

  mock_future = mock.Mock()
  mock_future.result.return_value = "message_id"
  mock_publisher_client.publish.return_value = mock_future

  result = message_tool.publish_message(
      topic_name, message, mock_credentials, tool_settings
  )

  assert result["message_id"] == "message_id"
  mock_get_publisher_client.assert_called_once()
  mock_publisher_client.publish.assert_called_once()


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(pubsub_v1.PublisherClient, "publish", autospec=True)
@mock.patch.object(pubsub_client_lib, "get_publisher_client", autospec=True)
def test_publish_message_with_ordering_key(
    mock_get_publisher_client, mock_publish
):
  """Test publish_message tool invocation with ordering_key."""
  topic_name = "projects/my_project_id/topics/my_topic"
  message = "Hello World"
  ordering_key = "key1"
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id="my_project_id")

  mock_publisher_client = mock.create_autospec(
      pubsub_v1.PublisherClient, instance=True
  )
  mock_get_publisher_client.return_value = mock_publisher_client

  mock_future = mock.Mock()
  mock_future.result.return_value = "message_id"
  mock_publisher_client.publish.return_value = mock_future

  result = message_tool.publish_message(
      topic_name,
      message,
      mock_credentials,
      tool_settings,
      ordering_key=ordering_key,
  )

  assert result["message_id"] == "message_id"
  mock_get_publisher_client.assert_called_once()
  mock_publisher_client.publish.assert_called_once()

  # Verify ordering_key was passed
  _, kwargs = mock_publisher_client.publish.call_args
  assert kwargs["ordering_key"] == ordering_key


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(pubsub_v1.PublisherClient, "publish", autospec=True)
@mock.patch.object(pubsub_client_lib, "get_publisher_client", autospec=True)
def test_publish_message_with_attributes(
    mock_get_publisher_client, mock_publish
):
  """Test publish_message tool invocation with attributes."""
  topic_name = "projects/my_project_id/topics/my_topic"
  message = "Hello World"
  attributes = {"key1": "value1", "key2": "value2"}
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id="my_project_id")

  mock_publisher_client = mock.create_autospec(
      pubsub_v1.PublisherClient, instance=True
  )
  mock_get_publisher_client.return_value = mock_publisher_client

  mock_future = mock.Mock()
  mock_future.result.return_value = "message_id"
  mock_publisher_client.publish.return_value = mock_future

  result = message_tool.publish_message(
      topic_name,
      message,
      mock_credentials,
      tool_settings,
      attributes=attributes,
  )

  assert result["message_id"] == "message_id"
  mock_get_publisher_client.assert_called_once()
  mock_publisher_client.publish.assert_called_once()

  # Verify attributes were passed
  _, kwargs = mock_publisher_client.publish.call_args
  assert kwargs["key1"] == "value1"
  assert kwargs["key2"] == "value2"


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(pubsub_v1.PublisherClient, "publish", autospec=True)
@mock.patch.object(pubsub_client_lib, "get_publisher_client", autospec=True)
def test_publish_message_exception(mock_get_publisher_client, mock_publish):
  """Test publish_message tool invocation when exception occurs."""
  topic_name = "projects/my_project_id/topics/my_topic"
  message = "Hello World"
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id="my_project_id")

  mock_publisher_client = mock.create_autospec(
      pubsub_v1.PublisherClient, instance=True
  )
  mock_get_publisher_client.return_value = mock_publisher_client

  # Simulate an exception during publish
  mock_publisher_client.publish.side_effect = Exception("Publish failed")

  result = message_tool.publish_message(
      topic_name,
      message,
      mock_credentials,
      tool_settings,
  )

  assert result["status"] == "ERROR"
  assert "Publish failed" in result["error_details"]
  mock_get_publisher_client.assert_called_once()
  mock_publisher_client.publish.assert_called_once()
