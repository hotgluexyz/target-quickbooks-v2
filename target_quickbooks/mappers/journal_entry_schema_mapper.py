from typing import Dict

from target_quickbooks.mappers.base_mapper import BaseMapper, InvalidInputError
from target_quickbooks.mappers.journal_entry_line_item_schema_mapper import JournalEntryLineItemSchemaMapper

class JournalEntrySchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        {"record_field": "journalEntryNumber", "qbo_field": "DocNumber", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "journalEntryNumber": "DocNumber",
        "transactionDate": "TxnDate",
        "description": "PrivateNote"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_currency(),
            **self._map_line_items()
        }

        if payload.get("CurrencyRef") and (exchange_rate := self.record.get("exchangeRate")):
            payload["ExchangeRate"] = exchange_rate

        self._map_fields(payload)

        return payload
    
    def _map_line_items(self):
        mapped_lines = []
        lines_amount_sum = 0

        if line_items := self.record.get("lineItems", []):
            for line in line_items:
                mapped_line = JournalEntryLineItemSchemaMapper(line, "JournalEntryLineItem", self.reference_data).to_quickbooks()
                mapped_lines.append(mapped_line)
                lines_amount_sum += mapped_line["Amount"] * (-1 if mapped_line["JournalEntryLineDetail"]["PostingType"] == "Credit" else 1)

        if lines_amount_sum != 0:
            raise InvalidInputError(f"The Journal is out of balance by {lines_amount_sum}. Please check that the Amount is correct for each line")

        return { "Line": mapped_lines }
