from typing import Dict, List

from hotglue_models_accounting.accounting import JournalEntry
from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.journal_entry_schema_mapper import JournalEntrySchemaMapper


class JournalEntrySink(QuickbooksBatchSink):
    name = "JournalEntries"
    record_type = "JournalEntry"
    unified_schema = JournalEntry
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing JournalEntries by Id or DocNumber
        # we have to perform two operations because QBO doesn't support the OR operator
        existing_journal_entries = []
        journal_entry_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        journal_entry_numbers = {f"'{record['journalEntryNumber']}'" for record in records if record.get("journalEntryNumber")}

        if journal_entry_ids:
            journal_entry_ids_str = ",".join(journal_entry_ids)
            existing_journal_entries += self.quickbooks_client.get_entities("JournalEntry", select_statement="Id, DocNumber, SyncToken", where_filter=f"Id in ({journal_entry_ids_str})")
        if journal_entry_numbers:
            journal_entry_numbers_str = ",".join(journal_entry_numbers)
            existing_journal_entries += self.quickbooks_client.get_entities("JournalEntry", select_statement="Id, DocNumber, SyncToken", where_filter=f"DocNumber in ({journal_entry_numbers_str})")

        # fetch vendors by Id and DisplayName
        existing_vendors = []
        vendor_ids = set()
        vendor_names = set()
        for record in records:
            vendor_ids.update({f"'{line_item['vendorId']}'" for line_item in record.get("lineItems", []) if line_item.get("vendorId")})
            vendor_names.update({line_item['vendorName'].replace("'", r"\'") for line_item in record.get("lineItems", []) if line_item.get("vendorName")})

        if vendor_ids:
            vendor_ids_str = ",".join(vendor_ids)
            existing_vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName, SyncToken", where_filter=f"Id in ({vendor_ids_str})")
        if vendor_names:
            vendor_names = {f"'{vendor_name}'" for vendor_name in vendor_names}
            vendor_names_str = ",".join(vendor_names)
            existing_vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName, SyncToken", where_filter=f"DisplayName in ({vendor_names_str})")

        # fetch customers by Id and DisplayName
        existing_customers = []
        customer_ids = set()
        customer_names = set()
        for record in records:
            customer_ids.update({f"'{line_item['customerId']}'" for line_item in record.get("lineItems", []) if line_item.get("customerId")})
            customer_names.update({line_item['customerName'].replace("'", r"\'") for line_item in record.get("lineItems", []) if line_item.get("customerName")})

        if customer_ids:
            customer_ids_str = ",".join(customer_ids)
            existing_customers += self.quickbooks_client.get_entities("Customer", select_statement="Id, DisplayName, SyncToken", where_filter=f"Id in ({customer_ids_str})")
        if customer_names:
            customer_names = {f"'{customer_name}'" for customer_name in customer_names}
            customer_names_str = ",".join(customer_names)
            existing_customers += self.quickbooks_client.get_entities("Customer", select_statement="Id, DisplayName, SyncToken", where_filter=f"DisplayName in ({customer_names_str})")

        return {
            **self._target.reference_data,
            self.name: existing_journal_entries,
            "Vendors": existing_vendors,
            "Customers": existing_customers
        }
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = JournalEntrySchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"

        if operation_type == "update":
            raise Exception("Update is not supported for JournalEntries. Skipping it.")

        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
    