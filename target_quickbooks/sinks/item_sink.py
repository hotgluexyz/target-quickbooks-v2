from typing import Dict, List

from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.item_schema_mapper import ItemSchemaMapper


class ItemSink(QuickbooksBatchSink):
    name = "Items"
    record_type = "Item"

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing items by Id or Name
        # we have to perform two operations because QBO doesn't support the OR operator
        existing_items = []
        item_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        item_names = {record['name'].replace("'", r"\'") for record in records if record.get("name")}

        if item_ids:
            item_ids_str = ",".join(item_ids)
            existing_items += self.quickbooks_client.get_entities("Item", select_statement="Id, Name, SyncToken", where_filter=f"Id in ({item_ids_str})")
        if item_names:
            item_names = {f"'{item_name}'" for item_name in item_names}
            item_names_str = ",".join(item_names)
            existing_items += self.quickbooks_client.get_entities("Item", select_statement="Id, Name, SyncToken", where_filter=f"Name in ({item_names_str})")

        existing_vendors = []
        vendor_ids = set()
        vendor_names = set()

        for record in records:
            vendor_ids.update({f"'{line_item['vendorId']}'" for line_item in record.get("itemVendors", []) if line_item.get("vendorId")})
            vendor_names.update({line_item['vendorName'].replace("'", r"\'") for line_item in record.get("itemVendors", []) if line_item.get("vendorName")})

        if vendor_ids:
            vendor_ids_str = ",".join(vendor_ids)
            existing_vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName", where_filter=f"Id in ({vendor_ids_str})")
        if vendor_names:
            vendor_names = {f"'{vendor_name}'" for vendor_name in vendor_names}
            vendor_names_str = ",".join(vendor_names)
            existing_vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName", where_filter=f"DisplayName in ({vendor_names_str})")

        return {**self._target.reference_data, self.name: existing_items, "Vendors": existing_vendors}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = ItemSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
    