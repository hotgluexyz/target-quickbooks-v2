from typing import Dict

from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound, InvalidInputError


class BillPaymentSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        {"record_field": "paymentNumber", "qbo_field": "DocNumber", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "paymentNumber": "DocNumber",
        "paymentDate": "TxnDate",
        "amount": "TotalAmt"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_line(),
            **self._map_vendor(),
            **self._map_currency(),
            **self._map_account()
        }
        
        if payload.get("CurrencyRef") and (exchange_rate := self.record.get("exchangeRate")):
            payload["ExchangeRate"] = exchange_rate

        self._map_fields(payload)

        return payload

    def _map_line(self):
        line = {
            "Line": [
                {
                    "Amount": self.record.get("amount"),
                    "LinkedTxn": [
                        {
                            "TxnId": self._map_bill(),
                            "TxnType": "Bill"
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
            if found_account["AccountType"] == "Credit Card":
                return {
                    "PayType": "CreditCard",
                    "CreditCardPayment": {
                        "CCAccountRef": {
                            "value": found_account["Id"],
                            "name": found_account.get("Name")
                        }
                    }
                }
            elif found_account["AccountType"] == "Bank" and found_account["AccountSubType"] == "Checking":
                return {
                    "PayType": "Check",
                    "CheckPayment": {
                        "BankAccountRef": {
                            "value": found_account["Id"],
                            "name": found_account.get("Name")
                        }
                    }
                }
            else:
                raise InvalidInputError(f"The account supplied should be of AccountType='Credit Card' or AccountType='Bank' and AccountSubType='Checking'")

        return {}

    def _map_bill(self):
        found_bill = None

        if bill_id := self.record.get("billId"):
            found_bill = next(
                (bill for bill in self.reference_data.get("Bills", [])
                if bill["Id"] == bill_id),
                None
            )

        if (bill_number := self.record.get("billNumber")) and found_bill is None:
            found_bill = next(
                (bill for bill in self.reference_data.get("Bills", [])
                if bill["DocNumber"] == bill_number),
                None
            )

        if found_bill is None:
            raise RecordNotFound(f"Bill could not be found in QBO with Id={bill_id} / Number={bill_number}")

        return found_bill["Id"]
