from typing import Dict, List

from target_quickbooks.base_sinks import QuickbooksBatchSink
from target_quickbooks.mappers.bill_payment_schema_mapper import BillPaymentSchemaMapper


class BillPaymentSink(QuickbooksBatchSink):
    name = "BillPayments"
    record_type = "BillPayment"

    def get_batch_reference_data(self, records: List) -> Dict:
        # get existing Bill payments by Id or DocNumber
        # we have to perform two operations because QBO doesn't support the OR operator
        bill_payments = []
        bill_payment_ids = {f"'{record['id']}'" for record in records if record.get("id")}
        bill_doc_numbers = {f"'{record['paymentNumber']}'" for record in records if record.get("paymentNumber")}

        if bill_payment_ids:
            bill_payment_ids_str = ",".join(bill_payment_ids)
            bill_payments += self.quickbooks_client.get_entities(self.record_type, select_statement="Id, DocNumber, SyncToken", where_filter=f"Id in ({bill_payment_ids_str})")
        if bill_doc_numbers:
            bill_doc_numbers_str = ",".join(bill_doc_numbers)
            bill_payments += self.quickbooks_client.get_entities(self.record_type, select_statement="Id, DocNumber, SyncToken", where_filter=f"DocNumber in ({bill_doc_numbers_str})")

        # get existing Bills by id or DocNumber
        bills = []
        bill_ids = {f"'{record['billId']}'" for record in records if record.get("billId")}
        bill_numbers = {f"'{record['billNumber']}'" for record in records if record.get("billNumber")}

        if bill_ids:
            bill_ids_str = ",".join(bill_ids)
            bills += self.quickbooks_client.get_entities("Bill", select_statement="Id, DocNumber, SyncToken", where_filter=f"Id in ({bill_ids_str})")
        if bill_numbers:
            bill_numbers_str = ",".join(bill_numbers)
            bills += self.quickbooks_client.get_entities("Bill", select_statement="Id, DocNumber, SyncToken", where_filter=f"DocNumber in ({bill_numbers_str})")

        # fetch vendors by Id and DisplayName
        vendors = []
        vendor_ids = {f"'{record['vendorId']}'" for record in records if record.get("vendorId")}
        vendor_names = {record['vendorName'].replace("'", r"\'") for record in records if record.get("vendorName")}

        if vendor_ids:
            vendor_ids_str = ",".join(vendor_ids)
            vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName, SyncToken", where_filter=f"Id in ({vendor_ids_str})")
        if vendor_names:
            vendor_names = {f"'{vendor_name}'" for vendor_name in vendor_names}
            vendor_names_str = ",".join(vendor_names)
            vendors += self.quickbooks_client.get_entities("Vendor", select_statement="Id, DisplayName, SyncToken", where_filter=f"DisplayName in ({vendor_names_str})")

        return {**self._target.reference_data, self.name: bill_payments, "Bills": bills, "Vendors": vendors}
    
    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        mapped_record = BillPaymentSchemaMapper(record, self.name, reference_data=reference_data).to_quickbooks()
        operation_type = "update" if "Id" in mapped_record else "create"
        return {"bId": f"{index}", "operation": operation_type, self.record_type: mapped_record}
