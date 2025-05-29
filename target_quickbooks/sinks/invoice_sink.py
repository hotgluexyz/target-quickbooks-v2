from typing import Dict, List

from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.invoice_line_item_schema_mapper import InvoiceSchemaMapper


class InvoiceSink(QuickbooksBatchSink):
    name = "Invoices"
    record_type = "Invoice"

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing invoices by id or DocNumber
        # we have to perform two operations because QBO doesn't support the OR operator
        invoices = []
        invoice_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        invoice_numbers = {f"'{record['invoiceNumber']}'" for record in records if record.get("invoiceNumber")}

        if invoice_ids:
            invoice_ids_str = ",".join(invoice_ids)
            invoices += self.quickbooks_client.get_entities("Invoice", select_statement="Id, DocNumber, SyncToken", where_filter=f"Id in ({invoice_ids_str})")
        if invoice_numbers:
            invoice_numbers_str = ",".join(invoice_numbers)
            invoices += self.quickbooks_client.get_entities("Invoice", select_statement="Id, DocNumber, SyncToken", where_filter=f"DocNumber in ({invoice_numbers_str})")

        # fetch customers by Id and DisplayName
        customers = []
        customer_ids = {f"'{record['customerId']}'" for record in records if record.get("customerId")}
        customer_names = {record['customerName'].replace("'", r"\'") for record in records if record.get("customerName")}

        if customer_ids:
            customer_ids_str = ",".join(customer_ids)
            customers += self.quickbooks_client.get_entities("Customer", select_statement="Id, DisplayName, SyncToken", where_filter=f"Id in ({customer_ids_str})")
        if customer_names:
            customer_names = {f"'{customer_name}'" for customer_name in customer_names}
            customer_names_str = ",".join(customer_names)
            customers += self.quickbooks_client.get_entities("Customer", select_statement="Id, DisplayName, SyncToken", where_filter=f"DisplayName in ({customer_names_str})")


        return {**self._target.reference_data, self.name: invoices, "Customers": customers}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = InvoiceSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
    