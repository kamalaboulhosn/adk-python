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

from google.auth.credentials import Credentials

from . import client
from .config import PubSubToolConfig


def list_topics(
    project_id: str, credentials: Credentials, settings: PubSubToolConfig
) -> list[str]:
  """List Pub/Sub topics in a Google Cloud project.

  Args:
      project_id (str): The Google Cloud project id.
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.

  Returns:
      list[str]: List of the Pub/Sub topic names present in the project.
  """
  try:
    publisher_client = client.get_publisher_client(
        credentials=credentials,
        user_agent=[settings.project_id, "list_topics"],
    )

    project_path = f"projects/{project_id}"
    topics = []
    for topic in publisher_client.list_topics(
        request={"project": project_path}
    ):
      topics.append(topic.name)
    return topics
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }


def get_topic(
    topic_name: str,
    credentials: Credentials,
    settings: PubSubToolConfig,
) -> dict:
  """Get metadata information about a Pub/Sub topic.

  Args:
      topic_name (str): The Pub/Sub topic name (e.g. projects/my-project/topics/my-topic).
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.

  Returns:
      dict: Dictionary representing the properties of the topic.
  """
  try:
    publisher_client = client.get_publisher_client(
        credentials=credentials,
        user_agent=[settings.project_id, "get_topic"],
    )
    topic = publisher_client.get_topic(request={"topic": topic_name})

    return {
        "name": topic.name,
        "labels": dict(topic.labels),
        "kms_key_name": topic.kms_key_name,
        "schema_settings": (
            str(topic.schema_settings) if topic.schema_settings else None
        ),
        "message_storage_policy": (
            str(topic.message_storage_policy)
            if topic.message_storage_policy
            else None
        ),
    }
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }


def list_subscriptions(
    project_id: str, credentials: Credentials, settings: PubSubToolConfig
) -> list[str]:
  """List Pub/Sub subscriptions in a Google Cloud project.

  Args:
      project_id (str): The Google Cloud project id.
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.

  Returns:
      list[str]: List of the Pub/Sub subscription names present in the project.
  """
  try:
    subscriber_client = client.get_subscriber_client(
        credentials=credentials,
        user_agent=[settings.project_id, "list_subscriptions"],
    )

    project_path = f"projects/{project_id}"
    subscriptions = []
    for subscription in subscriber_client.list_subscriptions(
        request={"project": project_path}
    ):
      subscriptions.append(subscription.name)
    return subscriptions
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }


def get_subscription(
    subscription_name: str,
    credentials: Credentials,
    settings: PubSubToolConfig,
) -> dict:
  """Get metadata information about a Pub/Sub subscription.

  Args:
      subscription_name (str): The Pub/Sub subscription name (e.g. projects/my-project/subscriptions/my-sub).
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.

  Returns:
      dict: Dictionary representing the properties of the subscription.
  """
  try:
    subscriber_client = client.get_subscriber_client(
        credentials=credentials,
        user_agent=[settings.project_id, "get_subscription"],
    )
    subscription = subscriber_client.get_subscription(
        request={"subscription": subscription_name}
    )

    return {
        "name": subscription.name,
        "topic": subscription.topic,
        "push_config": (
            str(subscription.push_config) if subscription.push_config else None
        ),
        "ack_deadline_seconds": subscription.ack_deadline_seconds,
        "retain_acked_messages": subscription.retain_acked_messages,
        "message_retention_duration": (
            str(subscription.message_retention_duration)
            if subscription.message_retention_duration
            else None
        ),
        "labels": dict(subscription.labels),
        "enable_message_ordering": subscription.enable_message_ordering,
        "expiration_policy": (
            str(subscription.expiration_policy)
            if subscription.expiration_policy
            else None
        ),
        "filter": subscription.filter,
        "dead_letter_policy": (
            str(subscription.dead_letter_policy)
            if subscription.dead_letter_policy
            else None
        ),
        "retry_policy": (
            str(subscription.retry_policy)
            if subscription.retry_policy
            else None
        ),
        "detached": subscription.detached,
    }
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }


def list_schemas(
    project_id: str, credentials: Credentials, settings: PubSubToolConfig
) -> list[str]:
  """List Pub/Sub schemas in a Google Cloud project.

  Args:
      project_id (str): The Google Cloud project id.
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.

  Returns:
      list[str]: List of the Pub/Sub schema names present in the project.
  """
  try:
    schema_client = client.get_schema_client(
        credentials=credentials,
        user_agent=[settings.project_id, "list_schemas"],
    )

    project_path = f"projects/{project_id}"
    schemas = []
    for schema in schema_client.list_schemas(request={"parent": project_path}):
      schemas.append(schema.name)
    return schemas
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }


def get_schema(
    schema_name: str,
    credentials: Credentials,
    settings: PubSubToolConfig,
) -> dict:
  """Get metadata information about a Pub/Sub schema.

  Args:
      schema_name (str): The Pub/Sub schema name (e.g. projects/my-project/schemas/my-schema).
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.

  Returns:
      dict: Dictionary representing the properties of the schema.
  """
  try:
    schema_client = client.get_schema_client(
        credentials=credentials,
        user_agent=[settings.project_id, "get_schema"],
    )
    schema = schema_client.get_schema(request={"name": schema_name})

    return {
        "name": schema.name,
        "type": str(schema.type_),
        "definition": schema.definition,
        "revision_id": schema.revision_id,
        "revision_create_time": str(schema.revision_create_time),
    }
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }


def list_schema_revisions(
    schema_name: str,
    credentials: Credentials,
    settings: PubSubToolConfig,
) -> list[str]:
  """List revisions of a Pub/Sub schema.

  Args:
      schema_name (str): The Pub/Sub schema name (e.g. projects/my-project/schemas/my-schema).
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.

  Returns:
      list[str]: List of the Pub/Sub schema revision IDs.
  """
  try:
    schema_client = client.get_schema_client(
        credentials=credentials,
        user_agent=[settings.project_id, "list_schema_revisions"],
    )

    revisions = []
    for schema in schema_client.list_schema_revisions(
        request={"name": schema_name}
    ):
      revisions.append(schema.revision_id)
    return revisions
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }


def get_schema_revision(
    schema_name: str,
    revision_id: str,
    credentials: Credentials,
    settings: PubSubToolConfig,
) -> dict:
  """Get metadata information about a specific Pub/Sub schema revision.

  Args:
      schema_name (str): The Pub/Sub schema name (e.g. projects/my-project/schemas/my-schema).
      revision_id (str): The revision ID of the schema.
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.

  Returns:
      dict: Dictionary representing the properties of the schema revision.
  """
  try:
    schema_client = client.get_schema_client(
        credentials=credentials,
        user_agent=[settings.project_id, "get_schema_revision"],
    )
    # The get_schema method can take a revision ID appended to the name
    # Format: projects/{project}/schemas/{schema}@{revision}
    name_with_revision = f"{schema_name}@{revision_id}"
    schema = schema_client.get_schema(request={"name": name_with_revision})

    return {
        "name": schema.name,
        "type": str(schema.type_),
        "definition": schema.definition,
        "revision_id": schema.revision_id,
        "revision_create_time": str(schema.revision_create_time),
    }
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }
