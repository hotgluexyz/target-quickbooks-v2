"""Colocated smoke-test runner for target-quickbooks-v2 (hotglue-smoke-test)."""

import vcr

from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetQuickBooksTestRunner(VCRTargetTestRunner):
    # QBO batch + entity plumbing and enum-like fields the target matches on replay.
    PRESERVE_KEYS = {
        "AccountSubType",
        "AccountType",
        "Classification",
        "DetailType",
        "Id",
        "Query",
        "SyncToken",
        "bId",
        "domain",
        "grant_type",
        "operation",
        "token_type",
    }
    # Literals the mappers compare. Scrubbing them in data.singer makes replay
    # drop phones, addresses, and inventory fields the cassette request still has.
    PRESERVE_VALUES = {
        "QBO",
        "primary",
        "secondary",
        "fax",
        "mobile",
        "billing",
        "shipping",
        "asset",
        "expense",
        "income",
        "Inventory",
        "Credit",
        "Debit",
        # Entity names also appear as dimension labels in singer input. Scrubbing
        # them rewrites `select * from Department` / `Class` in the cassette.
        "Class",
        "Department",
        # Bill-payment request literals. PayType and TxnType get replaced in the
        # cassette, then replay sends the real enum values from the mapper.
        "Bill",
        "Check",
        # Bank bill payments: account *name* "Checking" must not scrub AccountSubType.
        "Checking",
        "CreditCard",
        "Invoice",
        "Customer",
        "Vendor",
    }

    def module(self) -> str:
        return "target_quickbooks"

    def launch(self):
        from target_quickbooks.target import TargetQuickBooks

        TargetQuickBooks.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        return vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=list(self.FILTER_HEADERS),
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            before_record_response=self.before_record_response,
        )


if __name__ == "__main__":
    TargetQuickBooksTestRunner.main()
