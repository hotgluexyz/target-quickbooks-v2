from typing import Dict, List

from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.customer_schema_mapper import CustomerSchemaMapper


class CustomerSink(QuickbooksBatchSink):
    name = "Customers"
    record_type = "Customer"

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing customers and parent customers by id or DisplayName
        # we have to perform two operations because QBO doesn't support the OR operator
        customers = []
        customer_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        customer_ids.update({f"'{record['parentId']}'" for record in records if record.get("parentId")})
        customer_names = {record['fullName'].replace("'", r"\'") for record in records if record.get("fullName")}
        customer_names.update({record['parentName'].replace("'", r"\'") for record in records if record.get("parentName")})

        if customer_ids:
            customer_ids_str = ",".join(customer_ids)
            customers += self.quickbooks_client.get_entities("Customer", select_statement="Id, DisplayName, SyncToken", where_filter=f"Id in ({customer_ids_str})")
        if customer_names:
            customer_names = {f"'{customer_name}'" for customer_name in customer_names}
            customer_names_str = ",".join(customer_names)
            customers += self.quickbooks_client.get_entities("Customer", select_statement="Id, DisplayName, SyncToken", where_filter=f"DisplayName in ({customer_names_str})")

        return {**self._target.reference_data, self.name: customers}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = CustomerSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
    