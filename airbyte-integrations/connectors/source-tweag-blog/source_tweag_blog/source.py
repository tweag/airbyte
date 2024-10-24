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
import time
import signal
import logging


logger = logging.getLogger("airbyte")


# # Basic full refresh stream
class TweagBlogStream(HttpStream, ABC):
    """The Tweag Blog Stream"""

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
        """Return the token required to fetch the next page."""
        return None

    @property
    def http_method(self) -> str:
        """The HTTP method that should be used to make the request."""
        return "POST"

    def request_body_json(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        """Return a dictionary representing the JSON body of the request."""
        query = self.query
        return {"query": query}

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """Parse the response and return an iterator of result rows."""
        try:
            response_json = response.json()

            # Navigate to the data field
            data = response_json
            keys = self._data_field.split(".")
            
            if "edges" in response_json["data"]["allMarkdownRemark"]:
                for item in response_json["data"]["allMarkdownRemark"]["edges"]:
                    node = item["node"]
                    # Add fileAbsolutePath to frontmatter
                    node["frontmatter"]["fileAbsolutePath"] = node["fileAbsolutePath"]

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
        """Name of this stream."""
        return self._name

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        """The path to the API endpoint."""
        return self._path

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """Returns the primary key(s) of the stream as a tuple of strings."""
        return self._primary_key


# Source
class SourceTweagBlog(AbstractSource):
    """The Tweag Blog Data Source"""

    def clone_repo(self, repo_url: str, clone_dir: str):
        """Clone the repository to the specified directory"""
        if os.path.exists(clone_dir):
            subprocess.run(
                ["git", "checkout", "master"],
                cwd=clone_dir,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                ["git", "pull"],
                cwd=clone_dir,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        else:
            subprocess.run(
                ["git", "clone", repo_url, clone_dir],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                ["git", "checkout", "master"],
                cwd=clone_dir,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    def wait_for_server(self, url: str, timeout: int = 120) -> bool:
        """Wait for the Gatsby server to be ready."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    return True
            except requests.ConnectionError:
                time.sleep(5)  # Wait before retrying
        return False

    def start_gatsby_server(self, repo_dir: str) -> subprocess.Popen:
        """Start the Gatsby server"""
        if not os.path.exists(os.path.join(repo_dir, "node_modules", ".bin", "gatsby")):
            logger.info("Installing npm dependencies")
            subprocess.Popen(
                ["npm", "install", "--legacy-peer-deps"],
                cwd=repo_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ).wait()
        logger.info("Starting Gatsby server")
        gatsby_process = subprocess.Popen(
            ["npx", "gatsby", "develop", "-H", "0.0.0.0", "--port=12123"],
            cwd=repo_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return gatsby_process

    def free_port(self, port) -> None:
        """Free the port by killing the process using it"""
        cmd = f"lsof -t -i:{port}"
        process = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, _ = process.communicate()

        if stdout:
            # Port is in use, get the PID and terminate it
            pids = stdout.decode().strip().split("\n")
            for p in pids:
                if p:
                    os.kill(int(p), signal.SIGKILL)

    def stop_gatsby_server(self, gatsby_process) -> None:
        """Stop the Gatsby server"""
        gatsby_process.terminate()
        try:
            gatsby_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            gatsby_process.kill()
        self.free_port(12123)

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """Check if the source is reachable"""
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """Return a list of streams"""
        auth = None
        logger.info("Cloning repository")
        self.clone_repo(config["repo_url"], "/tmp/repo")
        time.sleep(20)
        self.free_port(12123)
        logger.info("Preparing for Gatsby server")
        self.start_gatsby_server("/tmp/repo")
        logger.info("Waiting for Gatsby server to start")
        if not self.wait_for_server("http://localhost:12123/__graphql", timeout=600):
            logger.error("Gatsby server did not start in time.")
            return []
        logger.info("Gatsby server started")
        return [TweagBlogStream(config=config, authenticator=auth)]
