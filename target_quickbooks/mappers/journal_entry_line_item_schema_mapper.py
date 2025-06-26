from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound, InvalidInputError

class JournalEntryLineItemSchemaMapper(BaseMapper):
    existing_record_pk_mappings = []

    field_mappings = {
        "description": "Description"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_item_based_line_details()
        }

        self._map_fields(payload)

        return payload

    def _map_item_based_line_details(self):
        customer_info = self._map_customer(ref_key="EntityRef")
        vendor_info = self._map_vendor(ref_key="EntityRef")

        details_info = {
            "DetailType": "JournalEntryLineDetail",
            "JournalEntryLineDetail": {
                **self._map_account(),
                **self._map_class(),
                **self._map_department()
            }
        }

        if customer_info:
            details_info["JournalEntryLineDetail"]["Entity"] = {**customer_info, "Type": "Customer"}
        elif vendor_info:
            details_info["JournalEntryLineDetail"]["Entity"] = {**vendor_info, "Type": "Vendor"}
        else:
            raise RecordNotFound("No customer or vendor found for the journal entry line item")

        field_mappings = {
            "entryType": "PostingType"
        }

        self._map_fields(details_info["JournalEntryLineDetail"], custom_field_mappings=field_mappings)

        entry_type = details_info["JournalEntryLineDetail"].get("PostingType")
        if entry_type not in ["Credit", "Debit"]:
            raise InvalidInputError(f"'{entry_type}' is an invalid field value for 'entryType'. It should one of 'Credit' or 'Debit'")

        if entry_type == "Credit":
            amount = abs(self.record.get("creditAmount", 0))
        else:
            amount = abs(self.record.get("debitAmount", 0))

        details_info["Amount"] = amount

        return details_info
