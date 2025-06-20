from typing import Dict, List

from hotglue_models_accounting.accounting import InvoicePayment
from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.invoice_payment_schema_mapper import InvoicePaymentSchemaMapper


class InvoicePaymentSink(QuickbooksBatchSink):
    name = "InvoicePayments"
    record_type = "Payment"
    unified_schema = InvoicePayment
    auto_validate_unified_schema = True

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing invoice payments by id or PaymentRefNum
        # we have to perform two operations because QBO doesn't support the OR operator
        invoice_payments = []
        invoice_payment_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        invoice_external_ids = {f"'{record['externalId']}'" for record in records if record.get("externalId")}

        if invoice_payment_ids:
            invoice_payment_ids_str = ",".join(invoice_payment_ids)
            invoice_payments += self.quickbooks_client.get_entities(self.record_type, select_statement="Id, PaymentRefNum, SyncToken", where_filter=f"Id in ({invoice_payment_ids_str})")
        if invoice_external_ids:
            invoice_external_ids_str = ",".join(invoice_external_ids)
            invoice_payments += self.quickbooks_client.get_entities(self.record_type, select_statement="Id, PaymentRefNum, SyncToken", where_filter=f"PaymentRefNum in ({invoice_external_ids_str})")

        # get existing invoices by id or DocNumber
        # TODO: refactor this into quickbooks_client.get_invoices(ids, numbers) because the same code is used in Invoice and Invoice payment sinks
        invoices = []
        invoice_ids = {f"'{record['invoiceId']}'" for record in records if record.get("invoiceId")}
        invoice_numbers = {f"'{record['invoiceNumber']}'" for record in records if record.get("invoiceNumber")}

        if invoice_ids:
            invoice_ids_str = ",".join(invoice_ids)
            invoices += self.quickbooks_client.get_entities("Invoice", select_statement="Id, DocNumber, SyncToken", where_filter=f"Id in ({invoice_ids_str})")
        if invoice_numbers:
            invoice_numbers_str = ",".join(invoice_numbers)
            invoices += self.quickbooks_client.get_entities("Invoice", select_statement="Id, DocNumber, SyncToken", where_filter=f"DocNumber in ({invoice_numbers_str})")

        # fetch customers by Id and DisplayName
        # TODO: refactor this into quickbooks_client.get_customers(ids, names) because the same code is used in Customer, Invoice and Invoice payment sinks
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

        return {**self._target.reference_data, self.name: invoice_payments, "Invoices": invoices, "Customers": customers}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = InvoicePaymentSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
    