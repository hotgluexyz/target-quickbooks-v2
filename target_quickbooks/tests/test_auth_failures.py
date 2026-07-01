import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from hotglue_etl_exceptions import InvalidCredentialsError
from pydantic import BaseModel

from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.quickbooks_client import QuickbooksClient


class FakeResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text
        self.content = text.encode("utf-8")
        self.headers = {}

    def json(self):
        return json.loads(self.text)


class FakeAuthRefreshError(Exception):
    def __init__(self, response):
        self.response = response
        super().__init__(response.text)


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


def test_quickbooks_client_raises_invalid_credentials_from_auth_status(tmp_path, monkeypatch):
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "refresh_token": "test_refresh_token",
        "access_token": "test_access_token",
        "redirect_uri": "https://app.hotglue.xyz/oauth/callback",
        "realmId": "4620816365164029070",
        "is_sandbox": True,
        "last_update": 0,
    }))

    class FakeAuthClient:
        def __init__(self, *args, **kwargs):
            self.access_token = None
            self.refresh_token = None

        def refresh(self, refresh_token):
            raise FakeAuthRefreshError(
                FakeResponse(400, '{"error":"invalid_grant","error_description":"Token invalid"}')
            )

    monkeypatch.setattr("target_quickbooks.quickbooks_client.AuthClient", FakeAuthClient)

    with pytest.raises(InvalidCredentialsError, match="invalid_grant"):
        QuickbooksClient(str(config_file), MagicMock())


def test_process_batch_writes_invalid_credentials_state_for_all_records(tmp_path):
    sink = DummyQuickbooksBatchSink(
        target=SimpleNamespace(
            logger=MagicMock(),
            config={},
            _state={},
            _latest_state={},
            initialization_error=InvalidCredentialsError("Token invalid"),
            quickbooks_client=None,
            reference_data={},
            incremental_target_state_path=str(tmp_path / "incremental-target-state.json"),
        ),
        stream_name="Invoices",
        schema={"type": "object", "properties": {"externalId": {"type": ["string", "null"]}}},
        key_properties=None,
    )

    sink.process_batch({"records": [{"externalId": "inv-1"}]})

    assert sink.latest_state["bookmarks"]["Invoices"] == [{
        "success": False,
        "error": "Token invalid",
        "hg_error_class": "InvalidCredentialsError",
        "externalId": "inv-1",
    }]
