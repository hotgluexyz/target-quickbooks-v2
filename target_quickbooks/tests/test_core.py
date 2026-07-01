import json
from unittest.mock import MagicMock

import pytest
from hotglue_etl_exceptions import InvalidCredentialsError

from target_quickbooks.quickbooks_client import QuickbooksClient
from target_quickbooks.target import TargetQuickBooks


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)
        self.content = self.text.encode("utf-8")
        self.headers = {}

    def json(self):
        return self._payload


class FakeAuthRefreshError(Exception):
    def __init__(self, response):
        self.response = response
        super().__init__(response.text)


def test_quickbooks_client_raises_invalid_credentials_for_invalid_grant(
    monkeypatch,
    quickbooks_config_file,
    logger,
):
    class FakeAuthClient:
        def __init__(self, *args, **kwargs):
            self.access_token = None
            self.refresh_token = None

        def refresh(self, refresh_token):
            raise FakeAuthRefreshError(
                FakeResponse(
                    400,
                    {
                        "error": "invalid_grant",
                        "error_description": "Token invalid",
                    },
                )
            )

    monkeypatch.setattr("target_quickbooks.quickbooks_client.AuthClient", FakeAuthClient)

    with pytest.raises(InvalidCredentialsError, match="Token invalid"):
        QuickbooksClient(str(quickbooks_config_file), logger)


def test_target_init_propagates_invalid_credentials_error_from_refresh_failure(
    monkeypatch,
    quickbooks_config_file,
):
    class FakeAuthClient:
        def __init__(self, *args, **kwargs):
            self.access_token = None
            self.refresh_token = None

        def refresh(self, refresh_token):
            raise FakeAuthRefreshError(
                FakeResponse(
                    400,
                    {
                        "error": "invalid_grant",
                        "error_description": "Token invalid",
                    },
                )
            )

    def fake_target_hotglue_init(self, config, parse_env_config=False, validate_config=True):
        self._config_file_path = config[0]
        self.logger = MagicMock()

    monkeypatch.setattr("target_quickbooks.quickbooks_client.AuthClient", FakeAuthClient)
    monkeypatch.setattr(
        "target_quickbooks.target.TargetHotglue.__init__",
        fake_target_hotglue_init,
    )

    with pytest.raises(InvalidCredentialsError, match="Token invalid"):
        TargetQuickBooks([str(quickbooks_config_file)])
