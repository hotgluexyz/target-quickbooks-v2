from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from hotglue_etl_exceptions import InvalidCredentialsError
from pydantic import BaseModel

from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.quickbooks_client import QuickbooksClient

from .test_core import FakeAuthRefreshError, FakeResponse


class DummyUnifiedSchema(BaseModel):
    externalId: str | None = None


class DummyQuickbooksBatchSink(QuickbooksBatchSink):
    name = "Invoices"
    endpoint = "/batch"
    base_url = "https://example.com"
    record_type = "Invoice"
    unified_schema = DummyUnifiedSchema

    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        return {"bId": f"{index}", "operation": "create", self.record_type: record}


@pytest.fixture
def dummy_target(tmp_path: Path, logger: MagicMock):
    return SimpleNamespace(
        logger=logger,
        config={},
        _state={},
        _latest_state={},
        quickbooks_client=None,
        reference_data={},
        incremental_target_state_path=str(tmp_path / "incremental-target-state.json"),
    )


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


def test_process_batch_writes_invalid_credentials_state_for_all_records(
    dummy_target,
):
    dummy_target.ensure_quickbooks_initialized = MagicMock(
        side_effect=InvalidCredentialsError("Token invalid")
    )

    sink = DummyQuickbooksBatchSink(
        target=dummy_target,
        stream_name="Invoices",
        schema={"type": "object", "properties": {"externalId": {"type": ["string", "null"]}}},
        key_properties=None,
    )

    sink.process_batch(
        {
            "records": [
                {"externalId": "inv-1"},
                {"externalId": "inv-2"},
            ]
        }
    )

    assert sink.latest_state["summary"]["Invoices"] == {
        "success": 0,
        "fail": 2,
        "existing": 0,
        "updated": 0,
    }
    assert sink.latest_state["bookmarks"]["Invoices"] == [
        {
            "success": False,
            "error": "Token invalid",
            "hg_error_class": "InvalidCredentialsError",
            "externalId": "inv-1",
        },
        {
            "success": False,
            "error": "Token invalid",
            "hg_error_class": "InvalidCredentialsError",
            "externalId": "inv-2",
        },
    ]
