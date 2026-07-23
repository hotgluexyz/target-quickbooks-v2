from typing import Dict, List

from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.time_activity_schema_mapper import TimeActivitySchemaMapper
from target_quickbooks.models.time_activity import TimeActivity
from target_quickbooks.util import pick_fields


class TimeActivitySink(QuickbooksBatchSink):
    name = "TimeActivities"
    record_type = "TimeActivity"
    unified_schema = TimeActivity
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, records: List) -> Dict:
        existing_time_activities = []
        time_activity_ids = {f"'{record['id']}'" for record in records if record.get("id")}

        if time_activity_ids:
            time_activity_ids_str = ",".join(time_activity_ids)
            existing_time_activities += self.quickbooks_client.get_entities(
                self.record_type,
                select_statement="Id, SyncToken",
                where_filter=f"Id in ({time_activity_ids_str})",
            )

        employees = []
        employee_ids = {f"'{record['employeeId']}'" for record in records if record.get("employeeId")}
        employee_names = {
            record["employeeName"].replace("'", r"\'")
            for record in records
            if record.get("employeeName")
        }

        if employee_ids:
            employee_ids_str = ",".join(employee_ids)
            employees += self.quickbooks_client.get_entities(
                "Employee",
                select_statement="Id, DisplayName",
                where_filter=f"Id in ({employee_ids_str})",
            )
        if employee_names:
            employee_names = {f"'{employee_name}'" for employee_name in employee_names}
            employee_names_str = ",".join(employee_names)
            employees += self.quickbooks_client.get_entities(
                "Employee",
                select_statement="Id, DisplayName",
                where_filter=f"DisplayName in ({employee_names_str})",
            )

        vendors = []
        vendor_ids = {f"'{record['vendorId']}'" for record in records if record.get("vendorId")}
        vendor_names = {
            record["vendorName"].replace("'", r"\'")
            for record in records
            if record.get("vendorName")
        }

        if vendor_ids:
            vendor_ids_str = ",".join(vendor_ids)
            vendors += self.quickbooks_client.get_entities(
                "Vendor",
                select_statement="Id, DisplayName",
                where_filter=f"Id in ({vendor_ids_str})",
            )
        if vendor_names:
            vendor_names = {f"'{vendor_name}'" for vendor_name in vendor_names}
            vendor_names_str = ",".join(vendor_names)
            vendors += self.quickbooks_client.get_entities(
                "Vendor",
                select_statement="Id, DisplayName",
                where_filter=f"DisplayName in ({vendor_names_str})",
            )

        customers = []
        customer_ids = {f"'{record['customerId']}'" for record in records if record.get("customerId")}
        customer_names = {
            record["customerName"].replace("'", r"\'")
            for record in records
            if record.get("customerName")
        }

        if customer_ids:
            customer_ids_str = ",".join(customer_ids)
            customers += self.quickbooks_client.get_entities(
                "Customer",
                select_statement="Id, DisplayName",
                where_filter=f"Id in ({customer_ids_str})",
            )
        if customer_names:
            customer_names = {f"'{customer_name}'" for customer_name in customer_names}
            customer_names_str = ",".join(customer_names)
            customers += self.quickbooks_client.get_entities(
                "Customer",
                select_statement="Id, DisplayName",
                where_filter=f"DisplayName in ({customer_names_str})",
            )

        items = []
        item_skus = {f"'{record['itemNumber']}'" for record in records if record.get("itemNumber")}
        item_ids = {f"'{record['itemId']}'" for record in records if record.get("itemId")}
        item_names = {
            record["itemName"].replace("'", r"\'")
            for record in records
            if record.get("itemName")
        }

        if item_ids:
            item_ids_str = ",".join(item_ids)
            items += self.quickbooks_client.get_entities(
                "Item",
                select_statement="Id, Name",
                where_filter=f"Id in ({item_ids_str})",
            )
        if item_skus:
            item_skus_str = ",".join(item_skus)
            items += [
                pick_fields(item, ["Id", "Name", "Sku"])
                for item in self.quickbooks_client.get_entities(
                    "Item",
                    select_statement="*",
                    where_filter=f"Sku in ({item_skus_str})",
                )
            ]
        if item_names:
            item_names = {f"'{item_name}'" for item_name in item_names}
            item_names_str = ",".join(item_names)
            items += self.quickbooks_client.get_entities(
                "Item",
                select_statement="Id, Name",
                where_filter=f"Name in ({item_names_str})",
            )

        return {
            **self._target.reference_data,
            self.name: existing_time_activities,
            "Employees": employees,
            "Vendors": vendors,
            "Customers": customers,
            "Items": items,
        }

    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = TimeActivitySchemaMapper(
            record, self.name, reference_data=reference_data
        ).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
