from typing import List

import requests
from dagster import ConfigurableResource


class HttpClient(ConfigurableResource):
    """Simple HTTP client for downloading text and JSONL."""

    timeout_seconds: int = 30

    def get_text(self, url: str) -> str:
        """Return response text for a given URL."""
        response = requests.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.text

    def get_jsonl_lines(self, url: str) -> List[str]:
        """Return non-empty lines from a JSONL URL."""
        text = self.get_text(url)
        return [line for line in text.splitlines() if line.strip()]
