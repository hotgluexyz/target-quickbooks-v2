from typing import Dict, List

from hotglue_models_accounting.accounting import PurchaseOrder
from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.purchase_order_schema_mapper import PurchaseOrderSchemaMapper


class PurchaseOrderSink(QuickbooksBatchSink):
    name = "PurchaseOrders"
    record_type = "PurchaseOrder"
    unified_schema = PurchaseOrder
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing PurchaseOrders by Id or DocNumber
        # we have to perform two operations because QBO doesn't support the OR operator
        existing_purchase_orders = []
        purchase_order_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        purchase_order_numbers = {f"'{record['purchaseOrderNumber']}'" for record in records if record.get("purchaseOrderNumber")}

        if purchase_order_ids:
            purchase_order_ids_str = ",".join(purchase_order_ids)
            existing_purchase_orders += self.quickbooks_client.get_entities("PurchaseOrder", select_statement="Id, DocNumber, SyncToken", where_filter=f"Id in ({purchase_order_ids_str})")
        if purchase_order_numbers:
            purchase_order_numbers_str = ",".join(purchase_order_numbers)
            existing_purchase_orders += self.quickbooks_client.get_entities("PurchaseOrder", select_statement="Id, DocNumber, SyncToken", where_filter=f"DocNumber in ({purchase_order_numbers_str})")

        # fetch vendors by Id and DisplayName
        existing_vendors = []
        vendor_ids = {f"'{record['vendorId']}'" for record in records if record.get("vendorId")}
        vendor_names = {record['vendorName'].replace("'", r"\'") for record in records if record.get("vendorName")}

        if vendor_ids:
            vendor_ids_str = ",".join(vendor_ids)
            existing_vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName, SyncToken", where_filter=f"Id in ({vendor_ids_str})")
        if vendor_names:
            vendor_names = {f"'{vendor_name}'" for vendor_name in vendor_names}
            vendor_names_str = ",".join(vendor_names)
            existing_vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName, SyncToken", where_filter=f"DisplayName in ({vendor_names_str})")

        # fetch customers by Id and DisplayName
        existing_customers = []
        customer_ids = set()
        customer_names = set()
        for record in records:
            customer_ids.update({f"'{line_item['projectId']}'" for line_item in record.get("lineItems", []) if line_item.get("projectId")})
            customer_names.update({line_item['projectName'].replace("'", r"\'") for line_item in record.get("lineItems", []) if line_item.get("projectName")})

        if customer_ids:
            customer_ids_str = ",".join(customer_ids)
            existing_customers += self.quickbooks_client.get_entities("Customer", select_statement="Id, DisplayName, SyncToken", where_filter=f"Id in ({customer_ids_str})")
        if customer_names:
            customer_names = {f"'{customer_name}'" for customer_name in customer_names}
            customer_names_str = ",".join(customer_names)
            existing_customers += self.quickbooks_client.get_entities("Customer", select_statement="Id, DisplayName, SyncToken", where_filter=f"DisplayName in ({customer_names_str})")

        # fetch items by Id and Name
        items = []
        item_ids = set()
        item_names = set()
        for record in records:
            item_ids.update({f"'{line_item['itemId']}'" for line_item in record.get("lineItems", []) if line_item.get("itemId")})
            item_names.update({line_item['itemName'].replace("'", r"\'") for line_item in record.get("lineItems", []) if line_item.get("itemName")})

        if item_ids:
            item_ids_str = ",".join(item_ids)
            items += self.quickbooks_client.get_entities("Item", select_statement="Id, Name", where_filter=f"Id in ({item_ids_str})")
        if item_names:
            item_names = {f"'{item_name}'" for item_name in item_names}
            item_names_str = ",".join(item_names)
            items += self.quickbooks_client.get_entities("Item", select_statement="Id, Name", where_filter=f"Name in ({item_names_str})")

        return {
            **self._target.reference_data,
            self.name: existing_purchase_orders,
            "Vendors": existing_vendors,
            "Customers": existing_customers,
            "Items": items
        }
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = PurchaseOrderSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
    