import pytest
from hotglue_etl_exceptions import InvalidCredentialsError

from target_quickbooks.quickbooks_client import QuickbooksClient

from .test_core import FakeAuthRefreshError, FakeResponse


def test_quickbooks_client_re_raises_non_credential_refresh_failures(
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
                    500,
                    {
                        "error": "server_error",
                        "error_description": "Intuit is unavailable",
                    },
                )
            )

    monkeypatch.setattr("target_quickbooks.quickbooks_client.AuthClient", FakeAuthClient)

    with pytest.raises(FakeAuthRefreshError, match="server_error"):
        QuickbooksClient(str(quickbooks_config_file), logger)


def test_quickbooks_client_reuses_cached_invalid_credentials_error_without_refreshing(
    logger,
):
    client = object.__new__(QuickbooksClient)
    client.logger = logger
    client.credentials_error = "Token invalid"

    class FakeAuthClient:
        def refresh(self, refresh_token):
            raise AssertionError("refresh should not be called once credentials are known invalid")

    client.auth_client = FakeAuthClient()
    client.config = {"refresh_token": "test_refresh_token"}

    with pytest.raises(InvalidCredentialsError, match="Token invalid"):
        client.update_access_token()
