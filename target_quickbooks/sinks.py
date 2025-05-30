import requests
import json
from target_quickbooks.client import QuickbooksSink
import re

from target_quickbooks.mapper import (
    customer_from_unified,
    vendor_from_unified,
    item_from_unified,
    invoice_from_unified,
    creditnote_from_unified,
    payment_method_from_unified,
    payment_term_from_unified,
    tax_rate_from_unified,
    department_from_unified,
    sales_receipt_from_unified,
    deposit_from_unified,
)


class InvoiceSink(QuickbooksSink):
    name = "Invoices"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        invoice = invoice_from_unified(
            record, self.customers, self.items, self.tax_codes, self.sales_terms
        )
        if record.get("id"):
            invoice_details = self.get_entities(
                "Invoice",
                check_active=False,
                fallback_key="Id",
                where_filter=f" id ='{record.get('id')}'",
            )
            if str(record.get("id")) in invoice_details:
                invoice.update(
                    {
                        "Id": record.get("id"),
                        "sparse": True,
                        "SyncToken": invoice_details[str(record.get("id"))][
                            "SyncToken"
                        ],
                    }
                )
                entry = ["Invoice", invoice, "update"]
            else:
                print(f"Invoice {record.get('id')} not found. Skipping...")
                return
        else:
            entry = ["Invoice", invoice, "create"]

            self.logger.info(json.dumps(entry))

        context["records"].append(entry)


class SalesReceiptSink(QuickbooksSink):
    name = "SalesReceipts"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        sales_receipt = sales_receipt_from_unified(
            record,
            self.customers,
            self.items,
            self.tax_codes,
        )
        if record.get("id"):
            receipt_details = self.get_entities(
                "SalesReceipt",
                check_active=False,
                fallback_key="Id",
                where_filter=f" id ='{record.get('id')}'",
            )
            if str(record.get("id")) in receipt_details:
                sales_receipt.update(
                    {
                        "Id": record.get("id"),
                        "sparse": True,
                        "SyncToken": receipt_details[str(record.get("id"))][
                            "SyncToken"
                        ],
                    }
                )
                entry = ["Sales Receipt", sales_receipt, "update"]
            else:
                print(f"Sales Receipt {record.get('id')} not found. Skipping...")
                return
        else:
            entry = ["SalesReceipt", sales_receipt, "create"]

            self.logger.info(json.dumps(entry))

        context["records"].append(entry)


class CustomerSink(QuickbooksSink):
    name = "Customers"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        customer = customer_from_unified(record)

        if record.get("salesTerm") and record.get("salesTerm") in self.terms:
            term = self.terms[record["salesTerm"]]
            customer["SalesTermRef"] = {"value": term["Id"]}

        # Get Customer Type
        if (
            record.get("customerType")
            and record.get("customerType") in self.customer_type
        ):
            customer_type = self.customer_type[record["customerType"]]
            customer["CustomerTypeRef"] = {"value": customer_type["Id"]}

        # Get Tax Code
        if record.get("taxCode") and record.get("taxCode") in self.tax_codes:
            tax_code = self.tax_codes[record["taxCode"]]
            customer["DefaultTaxCodeRef"] = {
                "value": tax_code["Id"],
                "name": tax_code["Name"],
            }

        # Get Payment Method
        if (
            record.get("paymentMethod")
            and record.get("paymentMethod") in self.payment_methods
        ):
            pm = self.payment_methods[record["paymentMethod"]]
            customer["PaymentMethodRef"] = {"value": pm["Id"], "name": pm["Name"]}

        if record.get("id"):
            customer_details = self.get_entities(
                "Customer",
                check_active=False,
                fallback_key="Id",
                where_filter=f" id ='{record.get('id')}'",
            )
            if str(record.get("id")) in customer_details:
                customer.update(
                    {
                        "Id": record.get("id"),
                        "sparse": True,
                        "SyncToken": customer_details[str(record.get("id"))][
                            "SyncToken"
                        ],
                    }
                )
                entry = ["Customer", customer, "update"]
            else:
                print(f"Customer {record.get('id')} not found. Skipping...")
                return
        elif customer.get("DisplayName") in self.customers:
            old_customer = self.customers[customer["DisplayName"]]
            customer["Id"] = old_customer["Id"]
            customer["SyncToken"] = old_customer["SyncToken"]
            customer["sparse"] = True
            entry = ["Customer", customer, "update"]
        else:
            entry = ["Customer", customer, "create"]

        context["records"].append(entry)


class VendorSink(QuickbooksSink):
    name = "Vendors"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        vendor = vendor_from_unified(record, self.tax_codes)

        if record.get("id"):
            vendor_details = self.get_entities(
                "Vendor",
                check_active=False,
                fallback_key="Id",
                where_filter=f" id ='{record.get('id')}'",
            )
            if str(record.get("id")) in vendor_details:
                vendor.update(
                    {
                        "Id": record.get("id"),
                        "sparse": True,
                        "SyncToken": vendor_details[str(record.get("id"))][
                            "SyncToken"
                        ],
                    }
                )
                entry = ["Vendor", vendor, "update"]
            else:
                print(f"Vendor {record.get('id')} not found. Skipping...")
                return
        elif vendor.get("DisplayName") in self.vendors:
            old_vendor = self.vendors[vendor["DisplayName"]]
            vendor["Id"] = old_vendor["Id"]
            vendor["SyncToken"] = old_vendor["SyncToken"]
            vendor["sparse"] = True
            entry = ["Vendor", vendor, "update"]
        else:
            entry = ["Vendor", vendor, "create"]

        context["records"].append(entry)

        self.logger.info(f"Generated record: {entry}")


class ItemSink(QuickbooksSink):
    name = "Items"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        item = item_from_unified(record, self.tax_codes, self.categories)

        # Have to include AssetAccountRef if we're creating an Inventory item
        if item.get("Type") == "Inventory":
            # TODO: Below is hardcoded
            item["AssetAccountRef"] = {"value": self.accounts["Inventory Asset"]["Id"]}

        # Convert account num -> accountRef
        income_account_num = item.pop("IncomeAccountNum", None)

        income_account = (
            self.accounts.get(income_account_num)
            if self.accounts.get(income_account_num)
            else self.accounts_name.get(income_account_num)
        )

        if income_account:
            item["IncomeAccountRef"] = {"value": income_account["Id"]}

        expense_account_num = item.pop("ExpenseAccountNum", None)

        expense_account = (
            self.accounts.get(expense_account_num)
            if self.accounts.get(expense_account_num)
            else self.accounts_name.get(expense_account_num)
        )

        if expense_account:
            item["ExpenseAccountRef"] = {"value": expense_account["Id"]}

        # Pick up account information from invoiceItem
        if not income_account and not expense_account and record.get("invoiceItem"):
            invoice_item = self.parse_objs(record.get("invoiceItem"))
            account_detail = self.accounts_name.get(invoice_item.get("accountName"))

            if account_detail is None:
                raise Exception(
                    f"Failed to find matching account with name: {invoice_item.get('accountName')}"
                )

            if account_detail.get("AccountType") == "Income":
                item["IncomeAccountRef"] = {"value": account_detail["Id"]}
            elif account_detail.get("AccountType") == "Expense":
                item["ExpenseAccountRef"] = {"value": account_detail["Id"]}

        if record.get("id"):
            item_details = self.get_entities(
                "Item",
                check_active=False,
                fallback_key="Id",
                where_filter=f" id ='{record.get('id')}'",
            )
            item_details_deleted = self.get_entities(
                "Item",
                check_active=False,
                fallback_key="Id",
                where_filter=f" id ='{record.get('id')}' and Active=false",
            )

            if item_details or item_details_deleted:
                if item_details:
                    item_previous = [x for x in item_details.values()]
                    item_previous = item_previous[0]
                else:
                    item_previous = [x for x in item_details_deleted.values()]
                    item_previous = item_previous[0]
                if str(record.get("id")) == item_previous["Id"]:
                    item.update(
                        {
                            "Id": record.get("id"),
                            "sparse": True,
                            "SyncToken": item_previous["SyncToken"],
                        }
                    )
                    entry = ["Item", item, "update"]
            else:
                print(f"Item {record.get('id')} not found. Skipping...")
                return

        elif item["Name"] in self.items:
            old_item = self.items[item["Name"]]
            item["Id"] = old_item["Id"]
            item["SyncToken"] = old_item["SyncToken"]
            entry = ["Item", item, "update"]
        else:
            entry = ["Item", item, "create"]

        context["records"].append(entry)


class CreditNoteSink(QuickbooksSink):
    name = "CreditNotes"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        creditnotes = creditnote_from_unified(
            record, self.customers, self.items, self.tax_codes
        )

        entry = ["CreditMemo", creditnotes, "create"]

        context["records"].append(entry)


class PaymentMethodSink(QuickbooksSink):
    name = "PaymentMethod"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        payment_methods = payment_method_from_unified(record)
        entry = ["PaymentMethod", payment_methods, "create"]

        context["records"].append(entry)


class PaymentTermSink(QuickbooksSink):
    name = "PaymentTerm"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        payment_terms = payment_term_from_unified(record)
        entry = ["Term", payment_terms, "create"]

        context["records"].append(entry)


class TaxRateSink(QuickbooksSink):
    name = "TaxRate"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        tax_rates = tax_rate_from_unified(record)
        entry = ["TaxService", tax_rates, "create"]

        context["records"].append(entry)


class DepartmentSink(QuickbooksSink):
    name = "Department"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        departments = department_from_unified(record)
        entry = ["Department", departments, "create"]

        context["records"].append(entry)


class JournalEntrySink(QuickbooksSink):
    name = "JournalEntries"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        # Get the journal entry id
        je_id = record["id"]

        line_items = []

        # Create line items
        for row in record.get("journalLines", record.get("lines", [])):
            if not "postingType" in row:
                # Add an error entry to the context to update the target state
                entry = ["JournalEntry", {
                    "id": je_id,
                    "error": f"Journal Entry {je_id} - you must define a postingType for each journalLine! Valid values are: Debit, Credit."
                }, "error"]
                context["records"].append(entry)
                return

            # Create journal entry line detail
            je_detail = {"PostingType": row["postingType"]}

            # Get the Quickbooks Account Ref
            acct_num = str(row["accountNumber"]) if row.get("accountNumber") else None
            acct_name = row.get("accountName")
            acct_ref = row.get("accountId")

            if acct_name and not acct_ref:
                acct_ref = self.accounts.get(
                    acct_num, self.accounts.get(acct_name, {})
                ).get("Id")

            if acct_ref is not None:
                je_detail["AccountRef"] = {"value": acct_ref}
            else:
                # Add an error entry to the context to update the target state
                entry = ["JournalEntry", {
                    "id": je_id,
                    "error": f"Account is missing on Journal Entry {je_id}! Name={acct_name} No={acct_num}"
                }, "error"]
                context["records"].append(entry)
                return

            # Get the Quickbooks Class Ref
            class_name = row.get("className")
            class_ref = self.classes.get(class_name, {}).get("Id")

            if class_ref is not None:
                je_detail["ClassRef"] = {"value": class_ref}
            else:
                self.logger.warning(
                    f"Class is missing on Journal Entry {je_id}! Name={class_name}"
                )

            # Get the Quickbooks Customer Ref
            customer_name = row.get("customerName")
            customer_ref = self.customers.get(customer_name, {}).get("Id")

            if customer_ref is not None:
                je_detail["Entity"] = {
                    "EntityRef": {"value": customer_ref},
                    "Type": "Customer",
                }
            else:
                self.logger.warning(
                    f"Customer is missing on Journal Entry {je_id}! Name={customer_name}"
                )

            # Get the Quickbooks Vendor Ref
            vendor_name = row.get("vendorName")
            vendor_ref = self.vendors.get(vendor_name, {}).get("Id")

            if vendor_ref is not None:
                je_detail["Entity"] = {
                    "EntityRef": {"value": vendor_ref},
                    "Type": "Vendor",
                }
            else:
                self.logger.warning(
                    f"Vendor is missing on Journal Entry {je_id}! Name={vendor_name}"
                )

            amount = row.get("amount")
            if not amount:
                entry = ["JournalEntry", {
                    "id": je_id,
                    "error": f"Journal entry line amount is missing on Journal Entry {je_id}"
                }, "error"]
                context["records"].append(entry)

            # Create the line item
            line_items.append(
                {
                    "Description": row.get("description"),
                    "Amount": abs(amount) if row.get("postingType") == "Credit" else amount,
                    "DetailType": "JournalEntryLineDetail",
                    "JournalEntryLineDetail": je_detail,
                }
            )

        # Create the [ resourceName , resource ]
        entry = {
            "TxnDate": record["transactionDate"],
            "DocNumber": je_id,
            "Line": line_items,
        }

        # Append the currency if provided
        if record.get("currency") is not None:
            entry["CurrencyRef"] = {"value": record["currency"]}

        entry = ["JournalEntry", entry, "create"]

        context["records"].append(entry)


class BillSink(QuickbooksSink):
    name = "Bills"

    def process_record(self, record: dict, context: dict) -> None:
        # Bill id
        bill_id = record.get("id")
        updating_bill = False
        if bill_id:
            bill_details = self.get_entities(
                "Bill",
                check_active=False,
                fallback_key="Id",
                where_filter=f" id ='{record.get('id')}'",
            )

            if bill_details:
                updating_bill = True

        if not context.get("records"):
            context["records"] = []
        entry = {}
        vendor = None
        skip_vendor = True
        line_items = []
        # NOTE: Departments aren't mapped yet
        # if record.get("department"):
        #     departments = self.get_departments()
        #     if record["department"] in departments:
        #         department = departments[record["department"]]
        #         entry["DepartmentRef"] = {
        #             "value": department["Id"],
        #             "name": department["Name"],
        #         }

        if "vendorName" in record:
            if record["vendorName"] in self.vendors:
                vendor = self.vendors[record["vendorName"]]
                skip_vendor = False
            else:
                skip_vendor = True

        # NOTE: We can proceed even without a Vendor if we are updating an existing Bill
        if skip_vendor == True and not updating_bill:
            raise Exception(f"A valid vendor is required for creating bill. No match found for {record.get('vendorName')}.")

        if vendor is not None:
            entry["VendorRef"] = {"value": vendor["Id"]}

        # Create line items
        for row in record.get("lineItems", []):
            # Create journal entry line detail
            line_detail = {}
            detail_type = "ItemBasedExpenseLineDetail"

            if row.get("taxCode"):
                tax_code = self.search_reference_data(
                    self.tax_codes, "Name", row.get("taxCode")
                ).get("Id")
                if tax_code:
                    line_detail["TaxCodeRef"] = {"value": tax_code}

            class_id = None
            if row.get("classId"):
                class_id = self.search_reference_data(
                    self.classes.values(), "Id", row.get("classId")
                ).get("Id")

            elif row.get("className"):
                class_id = self.search_reference_data(
                    self.classes.values(), "Name", row.get("className")
                ).get("Id")

            if class_id:
                line_detail["ClassRef"] = {
                    "value": class_id,
                }

            # Check if product name is provided
            if row.get("productName"):
                if row.get("productName") in self.items:
                    product_ref = self.items[row.get("productName")].get("Id")
                    line_detail["ItemRef"] = {"value": product_ref}
                    line_detail["UnitPrice"] = row.get("unitPrice")
                    line_detail["Qty"] = row.get("quantity")

            elif row.get("accountId"):
                detail_type = "AccountBasedExpenseLineDetail"
                line_detail["AccountRef"] = {"value": row.get("accountId")}
                line_detail["TaxAmount"] = row.get("taxAmount")

            elif row.get("accountName"):
                # Get the Quickbooks Account Ref
                # acct_num = str(row["accountName"])
                if row["accountName"] is not None:
                    acct_name = row["accountName"]
                    acct_ref = self.accounts.get(
                        acct_name, self.accounts.get(acct_name, {})
                    ).get("Id")
                detail_type = "AccountBasedExpenseLineDetail"
                line_detail["AccountRef"] = {"value": acct_ref}
                line_detail["TaxAmount"] = row.get("taxAmount")

                # missing in unified schema
                # if class_ref is not None:
                #     je_detail["ClassRef"] = {"value": class_ref}
                # else:
                #     self.logger.warning(
                #         f"Class is missing on Journal Entry {je_id}! Name={class_name}"
                #     )
            else:
                errored = True
                self.logger.error(
                    f"Account and product is missing on Bill {bill_id}! Skipping..."
                )
                return

            # Create the line item
            total_price = row.get("totalPrice")
            if not total_price and row.get("unitPrice") and row.get("quantity"):
                total_price = row["unitPrice"] * row["quantity"]

            if not total_price:
                errored = True
                self.logger.error(
                    f"Total price is missing on Bill {bill_id}! Skipping..."
                )
                return

            line_items.append(
                {
                    "Amount": total_price,
                    "DetailType": detail_type,
                    detail_type: line_detail,
                    "Description": row.get("description"),
                }
            )

        entry.update({"Id": bill_id, "Line": line_items})

        if record.get("dueDate"):
            entry["DueDate"] = record["dueDate"]

        # Append the currency if provided
        if record.get("currency"):
            entry["CurrencyRef"] = {"value": record["currency"]}

        if updating_bill:
            entry = ["Bill", entry, "update"]
        else:
            entry = ["Bill", entry, "create"]

        context["records"].append(entry)


class DepositsSink(QuickbooksSink):
    name = "Deposits"

    def _process_deposit(self, deposit):
        deposit = deposit_from_unified(deposit, self)
        entry = ["Deposit", deposit, "create"]
        return entry

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        generated_record = self._process_deposit(record)
        context["records"].append(generated_record)
        self.logger.info(f"Generated record: {generated_record}")


class BillPaymentsSink(QuickbooksSink):
    name = "BillPayments"

    def get_transaction(self, record, context):
        transaction_id = record.get("transactionId")
        transaction = self.get_entities(
            "Bill",
            check_active=False,
            fallback_key="Id",
            where_filter=f" id ='{transaction_id}'",
        )
        if not transaction:
            entry = ["BillPayments", {
                "error": f"Invalid transactionId={transaction_id}. Record={record}"
            }, "error"]
            context["records"].append(entry)
            return
        return transaction[transaction_id]

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        new_record = {
            "TotalAmt": record.get("amount"),
        }

        if not new_record.get("TotalAmt"):
            entry = ["BillPayments", {
                "error": f"Amount not provided. Record={record}"
            }, "error"]
            context["records"].append(entry)
            return
        
        if not record.get("transactionId"):
            entry = ["BillPayments", {
                "error": f"transactionId not provided. Record={record}"
            }, "error"]
            context["records"].append(entry)
            return

        vendor_id = record.get("vendorId")
        vendor_name = record.get("vendorName")
        transaction = None
        if vendor_id:
            vendor = next((vendor for vendor in self.vendors.values() if vendor.get("Id", None) == vendor_id), None)
            if not vendor:
                entry = ["BillPayments", {
                    "error": f"Invalid vendorId={vendor_id}. Record={record}"
                }, "error"]
                context["records"].append(entry)
                return
            new_record["VendorRef"] = {"value": vendor["Id"]}
        elif vendor_name:
            vendor = self.vendors.get(vendor_name)
            if vendor is None:
                entry = ["BillPayments", {
                    "error": f"Invalid vendorName={vendor_name}. Record={record}"
                }, "error"]
                context["records"].append(entry)
                return
            new_record["VendorRef"] = {"value": vendor["Id"]}
        else:
            transaction = self.get_transaction(record, context)
            # if transaction is not found, error was appended to context, return
            if not transaction:
                return
            new_record["VendorRef"] = {"value": transaction["VendorRef"]["value"]}
        
        if record.get("currency"):
            new_record["CurrencyRef"] = {"value": record["currency"]}
        else:
            # if currency not provided, get it from the linked transaction
            transaction = self.get_transaction(record, context) if not transaction else transaction
            # if transaction is not found, error was appended to context, return
            if not transaction:
                return
            new_record["CurrencyRef"] = {"value": transaction["CurrencyRef"]["value"]}

        account_id = record.get("accountId")
        account_name = record.get("accountName")
        account = None
        if account_id:
            account = next((account for account in self.accounts.values() if account.get("Id", None) == account_id), None)
        if account_name and account is None:
            account = self.accounts.get(account_name)
        if account is None:
            entry = ["BillPayments", {
                "error": f"accountId/accountName not found. Record={record}"
            }, "error"]
            context["records"].append(entry)
            return
        
        new_record["PayType"] = "CreditCard" if account["AccountType"] == "Credit Card" else "Check"

        if new_record["PayType"] == "CreditCard":
            new_record["CreditCardPayment"] = {
                "CCAccountRef": {
                    "value": account["Id"]
                }
            }
        else:
            new_record["CheckPayment"] = {
                "BankAccountRef": {
                    "value": account["Id"]
                }
            }

        new_record["Line"] = [{
            "Amount": new_record.get("TotalAmt"),
            "LinkedTxn": [
                {
                    "TxnId": record.get("transactionId"),
                    "TxnType": "Bill"
                }
            ]
        }]

        entry = ["BillPayment", new_record, "create"]
        context["records"].append(entry)
