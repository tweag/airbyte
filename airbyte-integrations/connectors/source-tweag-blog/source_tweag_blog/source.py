#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Generator, Union

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
    subprocess.Popen(["npx", "gatsby", "develop", "-H", "0.0.0.0", "--port=12123"], cwd=repo_dir)


def run_graphql_query(endpoint: str, query: str):
    response = requests.post(endpoint, json={'query': query})
    response.raise_for_status()
    return response.json()


# # Basic full refresh stream
# class TweagBlogStream(Stream):
#     def __init__(self, config: Mapping[str, Any]):
#         super().__init__(config=config)
#         self.endpoint = config["graphql_endpoint"]
#         self.query = config["graphql_query"]
#     def read_records(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_slice: Mapping[str, Any] = None) -> Generator[Mapping[str, Any], None, None]:
#         response = run_graphql_query(self.endpoint, self.query)
#         for record in response["data"]:
#             yield record
#     @property
#     def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
#         return self._primary_key

class TweagBlogStream(HttpStream, ABC):
    def __init__(self, name: str, path: str, primary_key: Union[str, List[str]], data_field: str, **kwargs: Any) -> None:
        self._name = name
        self._path = path
        self._primary_key = primary_key
        self._data_field = data_field
        super().__init__(**kwargs)

    url_base = "http://localhost:12123/__graphql"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {"include": "response_count,date_created,date_modified,language,question_count,analyze_url,preview,collect_stats"}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # content_type = response.headers.get('Content-Type')
        # if 'application/json' in content_type:
        #     try:
        #         response_json = response.json()
        #         yield from response_json.get("data", [])
        #     except requests.exceptions.JSONDecodeError:
        #         self.logger.error(f"Failed to parse response as JSON: {response.text}")
        #         raise
        # else:
        #     self.logger.error(f"Unexpected content type {content_type}: {response.text}")
        #     raise ValueError(f"Unexpected content type: {content_type}")
        html_content = response.text
        yield {"html_content": html_content}
        # response_json = response.json()
        # if self._data_field:
        #     yield from response_json.get(self._data_field, [])
        # else:
        #     yield from response_json

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
        start_gatsby_server("/tmp/repo")
        time.sleep(10)
        return [TweagBlogStream(name="tweag_blog", path="http://localhost:12123/__graphql", primary_key="id", data_field="data", authenticator=auth)]
