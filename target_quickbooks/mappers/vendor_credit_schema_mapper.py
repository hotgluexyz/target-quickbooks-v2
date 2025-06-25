from typing import Dict

from target_quickbooks.mappers.base_mapper import BaseMapper
from target_quickbooks.mappers.vendor_credit_line_item_schema_mapper import VendorCreditLineItemSchemaMapper
from target_quickbooks.mappers.vendor_credit_line_expense_schema_mapper import VendorCreditLineExpenseSchemaMapper

class VendorCreditSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        {"record_field": "vendorCreditNumber", "qbo_field": "DocNumber", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "vendorCreditNumber": "DocNumber",
        "issueDate": "TxnDate",
        "description": "PrivateNote"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_vendor(),
            **self._map_currency(),
            **self._map_department(),
            **self._map_line_items_and_expenses()
        }

        if payload.get("CurrencyRef") and (exchange_rate := self.record.get("exchangeRate")):
            payload["ExchangeRate"] = exchange_rate

        self._map_fields(payload)

        return payload
       
    def _map_line_items_and_expenses(self):
        mapped_lines = []

        if line_items := self.record.get("lineItems", []):
            for line in line_items:
                mapped_line = VendorCreditLineItemSchemaMapper(line, "VendorCreditLineItem", self.reference_data).to_quickbooks()
                mapped_lines.append(mapped_line)
        
        if expenses := self.record.get("expenses", []):
            for expense in expenses:
                mapped_line = VendorCreditLineExpenseSchemaMapper(expense, "VendorCreditLineExpense", self.reference_data).to_quickbooks()
                mapped_lines.append(mapped_line)

        return { "Line": mapped_lines }
