"""Tests for TLS handling in Client endpoints."""

import pytest

from azolla import Client, ClientConfig


@pytest.mark.asyncio
async def test_https_endpoint_not_supported() -> None:
    client = Client(ClientConfig(endpoint="https://localhost:52710"))
    with pytest.raises(ValueError):
        await client._ensure_connection()
