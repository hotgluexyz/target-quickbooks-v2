from typing import Dict, List

from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.vendor_schema_mapper import VendorSchemaMapper


class VendorSink(QuickbooksBatchSink):
    name = "Vendors"
    record_type = "Vendor"

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing vendors by id or DisplayName
        # we have to perform two operations because QBO doesn't support the OR operator
        existing_vendors = []
        vendor_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        vendor_names = {record['vendorName'].replace("'", r"\'") for record in records if record.get("vendorName")}

        if vendor_ids:
            vendor_ids_str = ",".join(vendor_ids)
            existing_vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName, SyncToken", where_filter=f"Id in ({vendor_ids_str})")
        if vendor_names:
            vendor_names = {f"'{vendor_name}'" for vendor_name in vendor_names}
            vendor_names_str = ",".join(vendor_names)
            existing_vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName, SyncToken", where_filter=f"DisplayName in ({vendor_names_str})")

        return {**self._target.reference_data, self.name: existing_vendors}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = VendorSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
    