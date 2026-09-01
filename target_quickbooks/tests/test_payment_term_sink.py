import pytest
from unittest.mock import MagicMock, patch

from target_quickbooks.client import QuickbooksSink
from target_quickbooks.sinks import PaymentTermSink


@pytest.fixture
def mock_payment_term_sink(mock_target):
    with patch.object(QuickbooksSink, "is_token_valid", return_value=True):
        with patch.object(QuickbooksSink, "get_reference_data"):
            sink = PaymentTermSink(
                target=mock_target,
                stream_name="PaymentTerm",
                schema={"properties": {}},
                key_properties=None,
            )

    sink.get_entities = MagicMock()
    sink.logger = MagicMock()
    sink.all_terms_by_name = {}
    return sink


class TestPaymentTermSink:
    def test_creates_new_payment_term(self, mock_payment_term_sink):
        context = {}
        record = {"name": "Net 30", "dueDays": 30}

        mock_payment_term_sink.process_record(record, context)

        assert len(context["records"]) == 1
        assert context["records"][0] == [
            "Term",
            {"Name": "Net 30", "DueDays": 30},
            "create",
        ]

    def test_updates_existing_payment_term_by_name(self, mock_payment_term_sink):
        context = {}
        record = {"name": "Net 30", "dueDays": 30}
        mock_payment_term_sink.all_terms_by_name = {
            "Net 30": {"Id": "3", "SyncToken": "1", "Name": "Net 30"}
        }

        mock_payment_term_sink.process_record(record, context)

        assert context["records"][0][2] == "update"
        assert context["records"][0][1]["Id"] == "3"
        assert context["records"][0][1]["SyncToken"] == "1"
        assert context["records"][0][1]["sparse"] is True

    def test_updates_existing_payment_term_by_id(self, mock_payment_term_sink):
        context = {}
        record = {"id": "3", "name": "Net 30", "dueDays": 30}
        mock_payment_term_sink.get_entities.return_value = {
            "3": {"Id": "3", "SyncToken": "2", "Name": "Net 30"}
        }

        mock_payment_term_sink.process_record(record, context)

        mock_payment_term_sink.get_entities.assert_called_once_with(
            "Term",
            key="Id",
            check_active=False,
            fallback_key="Id",
            where_filter=" id ='3'",
        )
        assert context["records"][0][2] == "update"
        assert context["records"][0][1]["Id"] == "3"
        assert context["records"][0][1]["SyncToken"] == "2"

    def test_skips_when_id_not_found(self, mock_payment_term_sink, capsys):
        context = {}
        record = {"id": "999", "name": "Net 30", "dueDays": 30}
        mock_payment_term_sink.get_entities.return_value = {}

        mock_payment_term_sink.process_record(record, context)

        captured = capsys.readouterr()
        assert "Payment term 999 not found. Skipping..." in captured.out
        assert context.get("records") is None or len(context.get("records", [])) == 0
