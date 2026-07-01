"""QuickBooks target class."""

import atexit

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from hotglue_etl_exceptions import InvalidCredentialsError

from target_quickbooks.quickbooks_client import QuickbooksClient
from target_quickbooks.sinks.bill_payment_sink import BillPaymentSink
from target_quickbooks.sinks.bill_sink import BillSink
from target_quickbooks.sinks.customer_sink import CustomerSink
from target_quickbooks.sinks.invoice_payment_sink import InvoicePaymentSink
from target_quickbooks.sinks.invoice_sink import InvoiceSink
from target_quickbooks.sinks.item_sink import ItemSink
from target_quickbooks.sinks.journal_entry_sink import JournalEntrySink
from target_quickbooks.sinks.purchase_order_sink import PurchaseOrderSink
from target_quickbooks.sinks.vendor_credit_sink import VendorCreditSink
from target_quickbooks.sinks.vendor_sink import VendorSink
from target_quickbooks.util import cleanup


class TargetQuickBooks(TargetHotglue):
    """Sample target for QuickBooks."""

    name = "target-quickbooks"
    MAX_PARALLELISM = 1
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("access_token", th.StringType, required=True),
        th.Property("redirect_uri", th.StringType, required=True),
        th.Property("realmId", th.StringType, required=True),
        th.Property("is_sanbox", th.BooleanType, required=False),
    ).to_dict()

    SINK_TYPES = [
        BillSink,
        BillPaymentSink,
        CustomerSink,
        InvoiceSink,
        InvoicePaymentSink,
        ItemSink,
        JournalEntrySink,
        VendorSink,
        VendorCreditSink,
        PurchaseOrderSink
    ]

    def __init__(
        self,
        config=None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None,
    ) -> None:
        self.config_file = config[0]
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        self.initialization_error = None
        self.quickbooks_client: QuickbooksClient = None
        self.reference_data = {}
        try:
            self.quickbooks_client = QuickbooksClient(self._config_file_path, self.logger)
            self.reference_data = self.get_reference_data()
        except InvalidCredentialsError as error:
            self.initialization_error = error

    def get_reference_data(self):
        self.logger.info("Getting reference data...")

        reference_data = {}
        reference_data["Accounts"] = self.quickbooks_client.get_entities("Account")
        reference_data["Departments"] = self.quickbooks_client.get_entities("Department")
        reference_data["PaymentMethods"] = self.quickbooks_client.get_entities("PaymentMethod")
        reference_data["CustomerTypes"] = self.quickbooks_client.get_entities("CustomerType")
        reference_data["TaxCodes"] = self.quickbooks_client.get_entities("TaxCode")
        reference_data["Currencies"] = self.quickbooks_client.get_entities("Currency")
        reference_data["Classes"] = self.quickbooks_client.get_entities("Class")
        reference_data["Terms"] = self.quickbooks_client.get_entities("Term")
        reference_data["ItemCategories"] = self.quickbooks_client.get_entities("Item", where_filter="Type='Category'")

        self.logger.info("Done getting reference data...")
        return reference_data


if __name__ == "__main__":
    atexit.register(cleanup)
    TargetQuickBooks.cli()
