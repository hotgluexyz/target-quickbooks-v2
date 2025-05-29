from typing import Dict

from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound
from target_quickbooks.mappers.invoice_line_item_schema_mapper import InvoiceLineItemSchemaMapper

class InvoiceSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        {"record_field": "invoiceNumber", "qbo_field": "DocNumber", "required_if_present": False}
    ]

    field_mappings = {
        "invoiceNumber": "DocNumber",
        "exchangeRate": "ExchangeRate",
        "issueDate": "TxnDate",
        "dueDate": "DueDate",
        "shipDate": "ShipDate",
        "notes": "PrivateNote"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_customer(),
            **self._map_description(),
            **self._map_currency(),
            **self._map_addresses(),
            **self._map_line_items(),
            **self._map_line_items()
        }

        self._map_fields(payload)

        return payload
    
    def _map_description(self):
        if description := self.record.get("description"):
            return {"CustomerMemo": { "value": description }}
        return {}
    
    def _map_tax_amount(self):
        if tax_amount := self.record.get("taxAmount"):
            return {"TxnTaxDetail": { "TotalTax": tax_amount }}
        return {}
    
    def _map_line_items(self):
        mapped_lines = []

        if lines := self.record.get("lineItems", []):
            for line in lines:
                mapped_line = InvoiceLineItemSchemaMapper(line, "InvoiceLineItem", self.reference_data).to_quickbooks()
                mapped_lines.append(mapped_line)
        
        return { "Line": mapped_lines }

