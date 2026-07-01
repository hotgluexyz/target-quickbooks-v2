import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def quickbooks_config() -> dict:
    return {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "refresh_token": "test_refresh_token",
        "access_token": "test_access_token",
        "redirect_uri": "https://app.hotglue.xyz/oauth/callback",
        "realmId": "4620816365164029070",
        "is_sandbox": True,
        "last_update": 0,
    }


@pytest.fixture
def quickbooks_config_file(tmp_path: Path, quickbooks_config: dict) -> Path:
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(quickbooks_config), encoding="utf-8")
    return config_file


@pytest.fixture
def logger() -> MagicMock:
    return MagicMock()
