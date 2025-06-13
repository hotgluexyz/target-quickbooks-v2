from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, InvalidInputError, RecordNotFound

class InvoiceLineItemSchemaMapper(BaseMapper):
    existing_record_pk_mappings = []

    field_mappings = {
        "description": "Description"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_sales_item_line_details()
        }

        self._map_fields(payload)

        return payload

    def _map_sales_item_line_details(self):
        details_info = {
            "DetailType": "SalesItemLineDetail",
            "SalesItemLineDetail": {
                **self._map_item(),
                **self._map_line_tax_code(),
                **self._map_class()
            }
        }

        field_mappings = {
            "discount": "DiscountAmt",
            "quantity": "Qty",
            "unitPrice": "UnitPrice",
            "serviceDate": "ServiceDate"
        }

        self._map_fields(details_info["SalesItemLineDetail"], custom_field_mappings=field_mappings)

        details_info["Amount"] = details_info["SalesItemLineDetail"].get("Qty", 1) * details_info["SalesItemLineDetail"].get("UnitPrice", 0)

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

    def _map_line_tax_code(self):
        tax_code_info = {}

        if tax_code := self.record.get("taxCode"):
            if tax_code not in ["TAX", "NON"]:
                raise InvalidInputError(f"Invalid value {tax_code} for line taxCode, it should be either 'TAX' or 'NON'")

            tax_code_info["TaxCodeRef"] = {
                "value": tax_code,
                "name": tax_code
            }

        return tax_code_info
