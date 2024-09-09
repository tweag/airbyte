#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import (
    Any,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    Generator,
    Union,
)

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.models import SyncMode
import subprocess
import os
import shutil
import time

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


def clone_repo(repo_url: str, clone_dir: str):
    if os.path.exists(clone_dir):
        shutil.rmtree(clone_dir)
    subprocess.run(["git", "clone", repo_url, clone_dir], check=True)
    subprocess.run(["git", "checkout", "master"], cwd=clone_dir, check=True)


def start_gatsby_server(repo_dir: str):
    subprocess.Popen(
        ["npx", "gatsby", "develop", "-H", "0.0.0.0", "--port=12123"], cwd=repo_dir
    )


def run_graphql_query(endpoint: str, query: str):
    response = requests.post(endpoint, json={"query": query})
    response.raise_for_status()
    return response.json()


# # Basic full refresh stream
class TweagBlogStream(HttpStream, ABC):
    def __init__(self, config: Mapping[str, Any], **kwargs: Any) -> None:
        self._name = config["name"]
        self._path = config["graphql_endpoint"]
        self._primary_key = "id"
        self._data_field = config["data_field"]
        self.query = config["graphql_query"]
        super().__init__(**kwargs)

    url_base = "http://localhost:12123/__graphql"

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    @property
    def http_method(self) -> str:
        return "POST"

    def request_body_json(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        # Define your GraphQL query here
        query = self.query
        return {"query": query}

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        try:
            response_json = response.json()

            # Navigate to the data field
            data = response_json
            keys = self._data_field.split(".")

            for key in keys:
                if isinstance(data, list):
                    # If data is a list, iterate over each element
                    data = [item.get(key, {}) for item in data]
                else:
                    # Otherwise, just get the key from the dictionary
                    data = data.get(key, {})

            # If the final result is a list, yield each item individually
            if isinstance(data, list):
                for item in data:
                    primary_key_value = f"{item.get('title')}_{item.get('author')}"
                    item["id"] = primary_key_value 
                    yield item
            else:
                # Otherwise, yield the single result
                primary_key_value = f"{data.get('title')}_{data.get('author')}"
                data["id"] = primary_key_value 
                yield data

        except requests.exceptions.JSONDecodeError:
            self.logger.error(f"Failed to parse response as JSON: {response.text}")
            raise

    @property
    def name(self) -> str:
        return self._name

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return self._path

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return self._primary_key


# Source
class SourceTweagBlog(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            clone_repo(config["repo_url"], "/tmp/repo")
            start_gatsby_server("/tmp/repo")
            return True, None
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # return [TweagBlogStream(config=config)]
        auth = None
        clone_repo(config["repo_url"], "/tmp/repo")
        time.sleep(20)
        start_gatsby_server("/tmp/repo")
        time.sleep(20)
        return [TweagBlogStream(config=config, authenticator=auth)]
