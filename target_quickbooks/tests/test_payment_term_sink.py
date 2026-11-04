from unittest.mock import MagicMock

import pytest

from target_quickbooks.sinks.payment_term_sink import PaymentTermSink


@pytest.fixture
def payment_term_sink():
    sink = PaymentTermSink.__new__(PaymentTermSink)
    sink.name = "paymentTerms"
    sink.record_type = "Term"
    sink._target = MagicMock()
    sink._target.reference_data = {"paymentTerms": []}
    sink.quickbooks_client = MagicMock()
    return sink


class TestPaymentTermSink:
    def test_creates_new_payment_term(self, payment_term_sink):
        batch_record = payment_term_sink.process_batch_record(
            {"name": "Net 30", "dueDays": 30},
            0,
            {"paymentTerms": []},
        )

        assert batch_record == {
            "bId": "0",
            "operation": "create",
            "Term": {"Name": "Net 30", "DueDays": 30},
        }

    def test_updates_existing_payment_term_by_name(self, payment_term_sink):
        reference_data = {
            "paymentTerms": [
                {"Id": "3", "SyncToken": "1", "Name": "Net 30", "Active": True}
            ]
        }

        batch_record = payment_term_sink.process_batch_record(
            {"name": "Net 30", "dueDays": 30},
            0,
            reference_data,
        )

        assert batch_record["operation"] == "update"
        assert batch_record["Term"]["Id"] == "3"
        assert batch_record["Term"]["SyncToken"] == "1"
        assert batch_record["Term"]["sparse"] is True

    def test_get_batch_reference_data_fetches_by_id_and_name(self, payment_term_sink):
        payment_term_sink.quickbooks_client.get_entities.side_effect = [
            [{"Id": "3", "Name": "Net 30", "SyncToken": "1", "Active": True}],
            [{"Id": "4", "Name": "Net 15", "SyncToken": "2", "Active": False}],
        ]

        reference_data = payment_term_sink.get_batch_reference_data(
            [
                {"id": "3", "name": "Net 30"},
                {"name": "Net 15"},
            ]
        )

        assert len(reference_data["paymentTerms"]) == 2
        assert payment_term_sink.quickbooks_client.get_entities.call_count == 2
