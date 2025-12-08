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

from typing import List
from typing import Optional

from google.auth.credentials import Credentials
from google.cloud import pubsub_v1

from . import client
from .config import PubSubToolConfig


def publish_message(
    topic_name: str,
    message: str,
    credentials: Credentials,
    settings: PubSubToolConfig,
    attributes: Optional[dict[str, str]] = None,
    ordering_key: Optional[str] = None,
) -> dict:
  """Publish a message to a Pub/Sub topic.

  Args:
      topic_name (str): The Pub/Sub topic name (e.g. projects/my-project/topics/my-topic).
      message (str): The message content to publish.
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.
      attributes (Optional[dict[str, str]]): Optional attributes to attach to the message.
      ordering_key (Optional[str]): Optional ordering key for the message.

  Returns:
      dict: Dictionary with the message_id of the published message.
  """
  try:
    publisher_options = None
    publish_kwargs = attributes or {}
    if ordering_key:
      publish_kwargs["ordering_key"] = ordering_key
      publisher_options = pubsub_v1.types.PublisherOptions(
          enable_message_ordering=True
      )

    publisher_client = client.get_publisher_client(
        credentials=credentials,
        user_agent=[settings.project_id, "publish_message"],
        publisher_options=publisher_options,
    )

    data = message.encode("utf-8")
    future = publisher_client.publish(topic_name, data, **publish_kwargs)
    message_id = future.result()

    return {"message_id": message_id}
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }


def pull_messages(
    subscription_name: str,
    credentials: Credentials,
    settings: PubSubToolConfig,
    max_messages: int = 1,
    auto_ack: bool = False,
) -> dict:
  """Pull messages from a Pub/Sub subscription.

  Args:
      subscription_name (str): The Pub/Sub subscription name (e.g. projects/my-project/subscriptions/my-sub).
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.
      max_messages (int): The maximum number of messages to pull. Defaults to 1.
      auto_ack (bool): Whether to automatically acknowledge the messages. Defaults to False.

  Returns:
      dict: Dictionary with the list of pulled messages.
  """
  try:
    subscriber_client = client.get_subscriber_client(
        credentials=credentials,
        user_agent=[settings.project_id, "pull_messages"],
    )

    response = subscriber_client.pull(
        subscription=subscription_name,
        max_messages=max_messages,
    )

    messages = []
    ack_ids = []
    for received_message in response.received_messages:
      messages.append({
          "message_id": received_message.message.message_id,
          "data": received_message.message.data.decode("utf-8"),
          "attributes": dict(received_message.message.attributes),
          "publish_time": str(received_message.message.publish_time),
          "ack_id": received_message.ack_id,
      })
      ack_ids.append(received_message.ack_id)

    if auto_ack and ack_ids:
      subscriber_client.acknowledge(
          subscription=subscription_name,
          ack_ids=ack_ids,
      )

    return {"messages": messages}
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }


def acknowledge_messages(
    subscription_name: str,
    ack_ids: List[str],
    credentials: Credentials,
    settings: PubSubToolConfig,
) -> dict:
  """Acknowledge messages on a Pub/Sub subscription.

  Args:
      subscription_name (str): The Pub/Sub subscription name (e.g. projects/my-project/subscriptions/my-sub).
      ack_ids (List[str]): List of acknowledgment IDs to acknowledge.
      credentials (Credentials): The credentials to use for the request.
      settings (PubSubToolConfig): The Pub/Sub tool settings.

  Returns:
      dict: Status of the operation.
  """
  try:
    subscriber_client = client.get_subscriber_client(
        credentials=credentials,
        user_agent=[settings.project_id, "acknowledge_messages"],
    )

    subscriber_client.acknowledge(
        subscription=subscription_name,
        ack_ids=ack_ids,
    )

    return {"status": "SUCCESS"}
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }
