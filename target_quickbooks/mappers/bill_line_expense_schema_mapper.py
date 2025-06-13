from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper


class BillLineExpenseSchemaMapper(BaseMapper):
    existing_record_pk_mappings = []

    field_mappings = {
        "description": "Description",
        "amount": "Amount"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_item_based_line_details()
        }

        self._map_fields(payload)

        return payload

    def _map_item_based_line_details(self):
        self.record["customerId"] = self.record.get("projectId")
        self.record["customerName"] = self.record.get("projectName")

        details_info = {
            "DetailType": "AccountBasedExpenseLineDetail",
            "AccountBasedExpenseLineDetail": {
                **self._map_account(),
                **self._map_customer(),
                **self._map_transaction_line_tax_code(),
                **self._map_class()
            }
        }

        return details_info
