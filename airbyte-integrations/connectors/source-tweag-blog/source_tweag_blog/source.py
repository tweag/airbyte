#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Generator

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.models import SyncMode
import subprocess

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
    subprocess.run(["git", "clone", repo_url, clone_dir], check=True)
    subprocess.run(["git", "checkout", "master"], cwd=clone_dir, check=True)


def start_gatsby_server(repo_dir: str):
    subprocess.Popen(["npx", "gatsby", "develop", "-H", "0.0.0.0", "--port=12123"], cwd=repo_dir)


def run_graphql_query(endpoint: str, query: str):
    response = requests.post(endpoint, json={'query': query})
    response.raise_for_status()
    return response.json()


# Basic full refresh stream
class TweagBlogStream(Stream):
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config)
        self.endpoint = config["graphql_endpoint"]
        self.query = config["graphql_query"]
    def read_records(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_slice: Mapping[str, Any] = None) -> Generator[Mapping[str, Any], None, None]:
        response = run_graphql_query(self.endpoint, self.query)
        for record in response["data"]:
            yield record


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
        return [TweagBlogStream(config=config)]
