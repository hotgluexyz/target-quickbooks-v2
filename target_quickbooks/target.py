"""QuickBooks target class."""

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue
from target_quickbooks.quickbooks_client import QuickbooksClient
from target_quickbooks.sinks.bill_sink import BillSink
from target_quickbooks.sinks.bill_payment_sink import BillPaymentSink
from target_quickbooks.sinks.customer_sink import CustomerSink
from target_quickbooks.sinks.invoice_sink import InvoiceSink
from target_quickbooks.sinks.invoice_payment_sink import InvoicePaymentSink
from target_quickbooks.sinks.item_sink import ItemSink
from target_quickbooks.sinks.journal_entry_sink import JournalEntrySink
from target_quickbooks.sinks.purchase_order_sink import PurchaseOrderSink
from target_quickbooks.sinks.vendor_sink import VendorSink


class TargetQuickBooks(TargetHotglue):
    """Sample target for QuickBooks."""

    name = "target-quickbooks"
    MAX_PARALLELISM = 1

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

        self.quickbooks_client: QuickbooksClient = QuickbooksClient(self._config_file_path, self.logger)
        self.reference_data = self.get_reference_data()

    def get_reference_data(self):
        self.logger.info(f"Getting reference data...")

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

        self.logger.info(f"Done getting reference data...")
        return reference_data


if __name__ == "__main__":
    TargetQuickBooks.cli()
