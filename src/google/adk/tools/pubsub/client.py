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
from typing import Union

from google.api_core.gapic_v1.client_info import ClientInfo
from google.auth.credentials import Credentials
from google.cloud import pubsub_v1

from ... import version

USER_AGENT = f"adk-pubsub-tool google-adk/{version.__version__}"


def get_publisher_client(
    *,
    credentials: Credentials,
    user_agent: Optional[Union[str, List[str]]] = None,
) -> pubsub_v1.PublisherClient:
  """Get a Pub/Sub Publisher client.

  Args:
    credentials: The credentials to use for the request.
    user_agent: The user agent to use for the request.

  Returns:
    A Pub/Sub Publisher client.
  """

  user_agents = [USER_AGENT]
  if user_agent:
    if isinstance(user_agent, str):
      user_agents.append(user_agent)
    else:
      user_agents.extend([ua for ua in user_agent if ua])

  client_info = ClientInfo(user_agent=" ".join(user_agents))

  publisher_client = pubsub_v1.PublisherClient(
      credentials=credentials,
      client_info=client_info,
  )

  return publisher_client


def get_subscriber_client(
    *,
    credentials: Credentials,
    user_agent: Optional[Union[str, List[str]]] = None,
) -> pubsub_v1.SubscriberClient:
  """Get a Pub/Sub Subscriber client.

  Args:
    credentials: The credentials to use for the request.
    user_agent: The user agent to use for the request.

  Returns:
    A Pub/Sub Subscriber client.
  """

  user_agents = [USER_AGENT]
  if user_agent:
    if isinstance(user_agent, str):
      user_agents.append(user_agent)
    else:
      user_agents.extend([ua for ua in user_agent if ua])

  client_info = ClientInfo(user_agent=" ".join(user_agents))

  subscriber_client = pubsub_v1.SubscriberClient(
      credentials=credentials,
      client_info=client_info,
  )

  return subscriber_client
