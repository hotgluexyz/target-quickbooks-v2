import pytest
from unittest.mock import MagicMock, patch

from target_quickbooks.client import QuickbooksSink
from target_quickbooks.sinks import CustomerSink


@pytest.fixture
def mock_customer_sink(mock_target):
    with patch.object(QuickbooksSink, "is_token_valid", return_value=True):
        with patch.object(QuickbooksSink, "get_reference_data"):
            sink = CustomerSink(
                target=mock_target,
                stream_name="Customers",
                schema={"properties": {}},
                key_properties=None,
            )

    sink.get_entities = MagicMock()
    sink.logger = MagicMock()
    sink.customers = {}
    sink.customer_type = {}
    sink.tax_codes = {}
    sink.payment_methods = {}
    sink.terms = {}
    return sink


class TestCustomerSink:
    def test_sets_sales_term_ref_for_active_term(self, mock_customer_sink):
        context = {}
        mock_customer_sink.terms = {
            "Net 30": {"Id": "3", "Name": "Net 30", "Active": True}
        }
        record = {
            "contactName": "Acme Corp",
            "salesTerm": "Net 30",
        }

        mock_customer_sink.process_record(record, context)

        assert context["records"][0][1]["SalesTermRef"] == {"value": "3"}

    def test_ignores_inactive_sales_term(self, mock_customer_sink):
        context = {}
        mock_customer_sink.terms = {}
        record = {
            "contactName": "Acme Corp",
            "salesTerm": "Old Term",
        }

        mock_customer_sink.process_record(record, context)

        assert "SalesTermRef" not in context["records"][0][1]
