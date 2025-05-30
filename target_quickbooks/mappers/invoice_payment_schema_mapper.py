from typing import Dict

from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound, InvalidInputError


class InvoicePaymentSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        {"record_field": "externalId", "qbo_field": "PaymentRefNum", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "PaymentRefNum",
        "paymentDate": "TxnDate",
        "exchangeRate": "ExchangeRate",
        "amount": "TotalAmt"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_line(),
            **self._map_customer(),
            **self._map_currency(),
            **self._map_account()
        }

        self._map_fields(payload)

        return payload

    def _map_line(self):
        line = {
            "Line": [
                {
                    "Amount": self.record.get("amount"),
                    "LinkedTxn": [
                        {
                            "TxnId": self._map_invoice(),
                            "TxnType": "Invoice"
                        }
                    ]
                }
            ]
        }

        return line

    def _map_account(self):
        found_account = None

        if account_id := self.record.get("accountId"):
            found_account = next(
                (account for account in self.reference_data.get("Accounts", [])
                if account["Id"] == account_id),
                None
            )

        if (account_name := self.record.get("accountName")) and found_account is None:
            found_account = next(
                (account for account in self.reference_data.get("Accounts", [])
                if account["Name"] == account_name),
                None
            )

        if (account_id or account_name) and found_account is None:
            raise RecordNotFound(f"Account could not be found in QBO with Id={account_id} / Name={account_name}")

        if found_account:
            if found_account["AccountType"] not in ["Other Current Asset", "Bank"]:
                raise InvalidInputError(f"The account supplied is of type={found_account['AccountType']}. It should be of type 'Other Current Asset' or 'Bank'")

            return {
                "DepositToAccountRef": {
                    "value": found_account["Id"],
                    "name": found_account["Name"]
                }
            }

        return {}

    def _map_invoice(self):
        found_invoice = None

        if invoice_id := self.record.get("invoiceId"):
            found_invoice = next(
                (invoice for invoice in self.reference_data.get("Invoices", [])
                if invoice["Id"] == invoice_id),
                None
            )

        if (invoice_number := self.record.get("invoiceNumber")) and found_invoice is None:
            found_invoice = next(
                (invoice for invoice in self.reference_data.get("Invoices", [])
                if invoice["DocNumber"] == invoice_number),
                None
            )

        if found_invoice is None:
            raise RecordNotFound(f"Invoice could not be found in QBO with Id={invoice_id} / Number={invoice_number}")

        return found_invoice["Id"]
