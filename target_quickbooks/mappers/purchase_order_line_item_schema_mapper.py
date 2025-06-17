from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound

class PurchaseOrderLineItemSchemaMapper(BaseMapper):
    existing_record_pk_mappings = []

    field_mappings = {
        "description": "Description"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_line_details()
        }

        self._map_fields(payload)

        return payload

    def _map_line_details(self):
        line_type = "ItemBasedExpenseLineDetail"

        account_ref = self._map_account()
        if account_ref:
            line_type = "AccountBasedExpenseLineDetail"
        
        self.record["customerId"] = self.record.get("projectId")
        self.record["customerName"] = self.record.get("projectName")

        details_info = {
            "DetailType": line_type,
            line_type: {
                **(account_ref if account_ref else self._map_item()),
                **self._map_customer(),
                **self._map_class()
            }
        }

        field_mappings = {
            "quantity": "Qty",
            "unitPrice": "UnitPrice"
        }

        self._map_fields(details_info[line_type], custom_field_mappings=field_mappings)

        if line_type == "AccountBasedExpenseLineDetail":
            details_info["Amount"] = self.record.get("amount")
        else:
            details_info["Amount"] = details_info[line_type].get("Qty", 1) * details_info[line_type].get("UnitPrice", 0)

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
