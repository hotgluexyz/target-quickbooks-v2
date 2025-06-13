from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound

class BillLineItemSchemaMapper(BaseMapper):
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
        self.record["customerId"] = self.record.get("projectId")
        self.record["customerName"] = self.record.get("projectName")

        details_info = {
            "DetailType": "ItemBasedExpenseLineDetail",
            "ItemBasedExpenseLineDetail": {
                **self._map_item(),
                **self._map_customer(),
                **self._map_transaction_line_tax_code(),
                **self._map_class()
            }
        }

        field_mappings = {
            "quantity": "Qty",
            "unitPrice": "UnitPrice"
        }

        self._map_fields(details_info["ItemBasedExpenseLineDetail"], custom_field_mappings=field_mappings)

        details_info["Amount"] = details_info["ItemBasedExpenseLineDetail"].get("Qty", 1) * details_info["ItemBasedExpenseLineDetail"].get("UnitPrice", 0)

        return details_info
    
    def _map_item(self):
        item_info = {}
        found_item = None

        if item_id := self.record.get("itemId"):
            found_item = next(
                (item for item in self.reference_data["Items"]
                if item["Id"] == item_id),
                None
            )

        if (item_name := self.record.get("itemName")) and not found_item:
            found_item = next(
                (item for item in self.reference_data["Items"]
                if item["Name"] == item_name),
                None
            )

        if (item_id or item_name) and found_item is None:
            raise RecordNotFound(f"An item with Id={item_id} / Name={item_name} could not be found in QBO")

        if found_item:
            item_info["ItemRef"] = {
                "value": found_item["Id"],
                "name": found_item["Name"]
            }

        return item_info
