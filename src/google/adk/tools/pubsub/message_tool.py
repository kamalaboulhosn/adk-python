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

from typing import Optional

from google.auth.credentials import Credentials

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
    publisher_client = client.get_publisher_client(
        credentials=credentials,
        user_agent=[settings.project_id, "publish_message"],
    )

    data = message.encode("utf-8")
    future = publisher_client.publish(
        topic_name, data, ordering_key=ordering_key, **(attributes or {})
    )
    message_id = future.result()

    return {"message_id": message_id}
  except Exception as ex:
    return {
        "status": "ERROR",
        "error_details": str(ex),
    }
