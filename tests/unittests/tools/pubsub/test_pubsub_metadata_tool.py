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
from google.adk.tools.pubsub import metadata_tool
from google.adk.tools.pubsub.config import PubSubToolConfig
from google.cloud import pubsub_v1
from google.oauth2.credentials import Credentials


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(pubsub_v1.PublisherClient, "list_topics", autospec=True)
@mock.patch.object(pubsub_client_lib, "get_publisher_client", autospec=True)
def test_list_topics(mock_get_publisher_client, mock_list_topics):
  """Test list_topics tool invocation."""
  project = "my_project_id"
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id=project)

  mock_publisher_client = mock.create_autospec(
      pubsub_v1.PublisherClient, instance=True
  )
  mock_get_publisher_client.return_value = mock_publisher_client
  mock_publisher_client.list_topics.return_value = [
      mock.Mock(name="projects/my_project_id/topics/topic1"),
      mock.Mock(name="projects/my_project_id/topics/topic2"),
  ]
  # Fix the mock names to return the string name when accessed
  mock_publisher_client.list_topics.return_value[0].name = "topic1"
  mock_publisher_client.list_topics.return_value[1].name = "topic2"

  result = metadata_tool.list_topics(project, mock_credentials, tool_settings)
  assert result == ["topic1", "topic2"]
  mock_get_publisher_client.assert_called_once()


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(pubsub_v1.PublisherClient, "get_topic", autospec=True)
@mock.patch.object(pubsub_client_lib, "get_publisher_client", autospec=True)
def test_get_topic(mock_get_publisher_client, mock_get_topic):
  """Test get_topic tool invocation."""
  topic_name = "projects/my_project_id/topics/my_topic"
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id="my_project_id")

  mock_publisher_client = mock.create_autospec(
      pubsub_v1.PublisherClient, instance=True
  )
  mock_get_publisher_client.return_value = mock_publisher_client

  mock_topic = mock.Mock()
  mock_topic.name = topic_name
  mock_topic.labels = {"key": "value"}
  mock_topic.kms_key_name = "key_name"
  mock_topic.schema_settings = "schema_settings"
  mock_topic.message_storage_policy = "storage_policy"

  mock_publisher_client.get_topic.return_value = mock_topic

  result = metadata_tool.get_topic(topic_name, mock_credentials, tool_settings)

  assert result["name"] == topic_name
  assert result["labels"] == {"key": "value"}
  mock_get_publisher_client.assert_called_once()


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(
    pubsub_v1.SubscriberClient, "list_subscriptions", autospec=True
)
@mock.patch.object(pubsub_client_lib, "get_subscriber_client", autospec=True)
def test_list_subscriptions(
    mock_get_subscriber_client, mock_list_subscriptions
):
  """Test list_subscriptions tool invocation."""
  project = "my_project_id"
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id=project)

  mock_subscriber_client = mock.create_autospec(
      pubsub_v1.SubscriberClient, instance=True
  )
  mock_get_subscriber_client.return_value = mock_subscriber_client
  mock_subscriber_client.list_subscriptions.return_value = [
      mock.Mock(name="projects/my_project_id/subscriptions/sub1"),
      mock.Mock(name="projects/my_project_id/subscriptions/sub2"),
  ]
  mock_subscriber_client.list_subscriptions.return_value[0].name = "sub1"
  mock_subscriber_client.list_subscriptions.return_value[1].name = "sub2"

  result = metadata_tool.list_subscriptions(
      project, mock_credentials, tool_settings
  )
  assert result == ["sub1", "sub2"]
  mock_get_subscriber_client.assert_called_once()


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(
    pubsub_v1.SubscriberClient, "get_subscription", autospec=True
)
@mock.patch.object(pubsub_client_lib, "get_subscriber_client", autospec=True)
def test_get_subscription(mock_get_subscriber_client, mock_get_subscription):
  """Test get_subscription tool invocation."""
  subscription_name = "projects/my_project_id/subscriptions/my_sub"
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id="my_project_id")

  mock_subscriber_client = mock.create_autospec(
      pubsub_v1.SubscriberClient, instance=True
  )
  mock_get_subscriber_client.return_value = mock_subscriber_client

  mock_subscription = mock.Mock()
  mock_subscription.name = subscription_name
  mock_subscription.topic = "projects/my_project_id/topics/my_topic"
  mock_subscription.push_config = "push_config"
  mock_subscription.ack_deadline_seconds = 10
  mock_subscription.retain_acked_messages = True
  mock_subscription.message_retention_duration = "duration"
  mock_subscription.labels = {"key": "value"}
  mock_subscription.enable_message_ordering = True
  mock_subscription.expiration_policy = "expiration"
  mock_subscription.filter = "filter"
  mock_subscription.dead_letter_policy = "dead_letter"
  mock_subscription.retry_policy = "retry"
  mock_subscription.detached = False

  mock_subscriber_client.get_subscription.return_value = mock_subscription

  result = metadata_tool.get_subscription(
      subscription_name, mock_credentials, tool_settings
  )

  assert result["name"] == subscription_name
  assert result["topic"] == "projects/my_project_id/topics/my_topic"
  mock_get_subscriber_client.assert_called_once()


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(pubsub_v1.SchemaServiceClient, "list_schemas", autospec=True)
@mock.patch.object(pubsub_client_lib, "get_schema_client", autospec=True)
def test_list_schemas(mock_get_schema_client, mock_list_schemas):
  """Test list_schemas tool invocation."""
  project = "my_project_id"
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id=project)

  mock_schema_client = mock.create_autospec(
      pubsub_v1.SchemaServiceClient, instance=True
  )
  mock_get_schema_client.return_value = mock_schema_client
  mock_schema_client.list_schemas.return_value = [
      mock.Mock(name="projects/my_project_id/schemas/schema1"),
      mock.Mock(name="projects/my_project_id/schemas/schema2"),
  ]
  mock_schema_client.list_schemas.return_value[0].name = "schema1"
  mock_schema_client.list_schemas.return_value[1].name = "schema2"

  result = metadata_tool.list_schemas(project, mock_credentials, tool_settings)
  assert result == ["schema1", "schema2"]
  mock_get_schema_client.assert_called_once()


@mock.patch.dict(os.environ, {}, clear=True)
@mock.patch.object(pubsub_v1.SchemaServiceClient, "get_schema", autospec=True)
@mock.patch.object(pubsub_client_lib, "get_schema_client", autospec=True)
def test_get_schema(mock_get_schema_client, mock_get_schema):
  """Test get_schema tool invocation."""
  schema_name = "projects/my_project_id/schemas/my_schema"
  mock_credentials = mock.create_autospec(Credentials, instance=True)
  tool_settings = PubSubToolConfig(project_id="my_project_id")

  mock_schema_client = mock.create_autospec(
      pubsub_v1.SchemaServiceClient, instance=True
  )
  mock_get_schema_client.return_value = mock_schema_client

  mock_schema = mock.Mock()
  mock_schema.name = schema_name
  mock_schema.type_ = "AVRO"
  mock_schema.definition = "definition"
  mock_schema.revision_id = "revision_id"
  mock_schema.revision_create_time = "time"

  mock_schema_client.get_schema.return_value = mock_schema

  result = metadata_tool.get_schema(
      schema_name, mock_credentials, tool_settings
  )

  assert result["name"] == schema_name
  assert result["type"] == "AVRO"
  mock_get_schema_client.assert_called_once()
