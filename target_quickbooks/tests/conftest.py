import pytest
from unittest.mock import MagicMock, patch
from target_quickbooks.client import QuickbooksSink
from target_quickbooks.sinks import InvoiceSink
from target_quickbooks.target import TargetQuickBooks


@pytest.fixture
def mock_config():
    return {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "refresh_token": "test_refresh_token",
        "access_token": "test_access_token",
        "expires_in": 3600,
        "start_date": "2005-01-01T00:00:00Z",
        "redirect_uri": "https://qa.hotglue.xyz/callback",
        "api_type": "BULK",
        "select_fields_by_default": True,
        "api_key": "test_api_key",
        "api_url": "https://qa.client-api.hotglue.xyz/v2/entity/auth",
        "bgColor": "#000",
        "domain": "hotglue.xyz",
        "entity_id": "quickbooks:sandbox",
        "entity_type": "connector",
        "flowVersion": "2",
        "hideText": True,
        "realmId": "4620816365164029070",
        "skipCallbackOAuthErrors": True,
        "is_sandbox": True
    }

@pytest.fixture
def mock_invoice_dict():
    return {
        "id": "1",
        "amountDue": "1234.56",
        "currency": "USD",
        "createdAt": "2024-09-27T02:00:00Z",
        "customerId": "1",
        "customerName": "John Doe",
        "invoiceNumber": "20-2-202408-5",
        "lineItems": [
            {
                "quantity": 1,
                "unitPrice": "1234.56",
                "totalPrice": "1234.56",
                "productName": "Design"
            }
        ],
        "customFields": [
            {
                "name": "connector",
                "value": "quickbooks"
            },
            {
                "name": "ownerEmail",
                "value": "johndoe@test.com"
            },
            {
                "name": "ownerId",
                "value": "1"
            },
            {
                "name": "invoiceDate",
                "value": "2024-8-31"
            }
        ]
    }

@pytest.fixture
def mock_data_invoice():
    return """{"type": "SCHEMA", "stream": "Invoices", "schema": {"type": ["object", "null"], "properties": {"id": {"type": ["string", "null"]}, "amountDue": {"type": ["string", "null"]}, "currency": {"type": ["string", "null"]}, "createdAt": {"type": ["string", "null"]}, "customerId": {"type": ["string", "null"]}, "customerName": {"type": ["string", "null"]}, "invoiceNumber": {"type": ["string", "null"]}, "lineItems": {"type": ["array", "null"], "items": {"type": ["object", "null"], "properties": {"quantity": {"type": ["integer", "null"]}, "unitPrice": {"type": ["string", "null"]}, "totalPrice": {"type": ["string", "null"]}, "productName": {"type": ["string", "null"]}}}}, "customFields": {"type": ["array", "null"], "items": {"type": ["object", "null"], "properties": {"name": {"type": ["string", "null"]}, "value": {"type": ["string", "null"]}}}}}}, "key_properties": ["id"]}
{"type": "RECORD", "stream": "invoices", "record": {"id": "5", "amountDue": "1923.06", "currency": "USD", "createdAt": "2024-09-23T01:00:37Z", "customerId": "2", "customerName": "Phong Tran 2", "invoiceNumber": "20-2-202408-5", "lineItems": [{"quantity": 1, "unitPrice": "1923.06", "totalPrice": "1923.06", "productName": "Design"}], "customFields": [{"name": "connector", "value": "quickbooks"}, {"name": "ownerEmail", "value": "phongtc73@yahoo.com"}, {"name": "ownerId", "value": "2650"}, {"name": "invoiceDate", "value": "2024-8-31"}]}}
{"type": "STATE", "value": {}}
"""

@pytest.fixture
def mock_target(mock_config):
    return TargetQuickBooks(mock_config)

@pytest.fixture
def mock_invoice_sink(mock_target):
    # Mock dependencies and external methods
    with patch.object(QuickbooksSink, "is_token_valid", return_value=True):
        with patch.object(QuickbooksSink, "get_reference_data"):
            mock_sink = InvoiceSink(target=mock_target, stream_name="Invoices", schema={"properties": {}}, key_properties=None)

    # Mock external methods that make API calls
    mock_sink.make_request = MagicMock()
    mock_sink.get_entities = MagicMock()
    mock_sink.logger = MagicMock()

    # Mock reference data that would normally be fetched from QuickBooks
    mock_sink.customers = {"customers_name": {"Id": "1"}}
    mock_sink.items = {"Design": {"Id": "1"}}
    mock_sink.tax_codes = {}
    mock_sink.sales_terms = {}

    return mock_sink
