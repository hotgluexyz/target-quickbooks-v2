from typing import Dict

from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound
from target_quickbooks.mappers.invoice_line_item_schema_mapper import InvoiceLineItemSchemaMapper

class InvoiceSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        {"record_field": "invoiceNumber", "qbo_field": "DocNumber", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
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
            **self._map_tax_code()
        }

        self._map_discount(payload)
        self._map_fields(payload)

        return payload
    
    def _map_description(self):
        if description := self.record.get("description"):
            return {"CustomerMemo": { "value": description }}
        return {}
    
    def _map_tax_code(self):
        tax_code_info = {}
        found_tax_code = None

        if tax_code := self.record.get("taxCode"):
            found_tax_code = next(
                (tax for tax in self.reference_data["TaxCodes"]
                if tax["Name"] == tax_code),
                None
            )

        if tax_code and found_tax_code is None:
            raise RecordNotFound(f"A TaxCode with Name={tax_code} could not be found in QBO")

        if found_tax_code:
            tax_code_info["TxnTaxDetail"] = {
                "TxnTaxCodeRef": {
                    "value": found_tax_code["Id"],
                    "name": found_tax_code["Name"]
                }
            }

        return tax_code_info

    def _map_line_items(self):
        mapped_lines = []

        if lines := self.record.get("lineItems", []):
            for line in lines:
                mapped_line = InvoiceLineItemSchemaMapper(line, "InvoiceLineItem", self.reference_data).to_quickbooks()
                mapped_lines.append(mapped_line)
        
        return { "Line": mapped_lines }

    def _map_discount(self, payload):
        if discount := self.record.get("discountAmount"):
            discount_line = {
                "DetailType": "DiscountLineDetail",
                "Amount": discount,
                "DiscountLineDetail": {
                    "PercentBased": False
                }
            }
            existing_lines = payload.get("Line", [])
            existing_lines.append(discount_line)
            payload["Line"] = existing_lines

