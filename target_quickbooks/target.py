"""QuickBooks target class."""

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_quickbooks.sinks import (
    BillSink,
    ItemSink,
    InvoiceSink,
    TaxRateSink,
    CustomerSink,
    CreditNoteSink,
    DepartmentSink,
    PaymentTermSink,
    JournalEntrySink,
    PaymentMethodSink,
    SalesReceiptSink,
    DepositsSink
)


class TargetQuickBooks(TargetHotglue):
    """Sample target for QuickBooks."""

    name = "target-quickbooks"
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
        ItemSink,
        InvoiceSink,
        TaxRateSink,
        CustomerSink,
        CreditNoteSink,
        DepartmentSink,
        PaymentTermSink,
        JournalEntrySink,
        PaymentMethodSink,
        SalesReceiptSink,
        DepositsSink
    ]


if __name__ == "__main__":
    TargetQuickBooks.cli()
