import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from hotglue_etl_exceptions import InvalidCredentialsError
from pydantic import BaseModel

from target_quickbooks.base_sinks import QuickbooksBatchSink
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
def quickbooks_config_file(tmp_path: Path) -> Path:
    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "refresh_token": "test_refresh_token",
                "access_token": "test_access_token",
                "redirect_uri": "https://app.hotglue.xyz/oauth/callback",
                "realmId": "4620816365164029070",
                "is_sandbox": True,
                "last_update": 0,
            }
        ),
        encoding="utf-8",
    )
    return config_file


def test_quickbooks_client_raises_invalid_credentials_for_invalid_grant(
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
                    {"error": "invalid_grant", "error_description": "Token invalid"},
                )
            )

    monkeypatch.setattr("target_quickbooks.quickbooks_client.AuthClient", FakeAuthClient)

    with pytest.raises(InvalidCredentialsError, match="Token invalid"):
        QuickbooksClient(str(quickbooks_config_file), MagicMock())


def test_target_init_stores_invalid_credentials_error(monkeypatch, quickbooks_config_file):
    def fake_target_hotglue_init(self, config, parse_env_config=False, validate_config=True):
        self._config_file_path = config[0]
        self._config = {}

    monkeypatch.setattr(
        "target_quickbooks.target.TargetHotglue.__init__",
        fake_target_hotglue_init,
    )
    monkeypatch.setattr(
        "target_quickbooks.target.QuickbooksClient",
        MagicMock(side_effect=InvalidCredentialsError("Token invalid")),
    )

    target = TargetQuickBooks([str(quickbooks_config_file)])

    assert target.quickbooks_client is None
    assert target.reference_data == {}
    assert str(target.initialization_error) == "Token invalid"


def test_process_batch_writes_invalid_credentials_state_for_all_records(tmp_path: Path):
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

    sink.process_batch({"records": [{"externalId": "inv-1"}, {"externalId": "inv-2"}]})

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
