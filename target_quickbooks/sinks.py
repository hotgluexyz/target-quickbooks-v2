import requests
import json
from target_quickbooks.client import QuickbooksSink
import re

from target_quickbooks.mapper import (
    customer_from_unified,
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
    alias_name = "invoices"

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
    alias_name = "salesreceipts"

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
    alias_name = "customers"

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


class ItemSink(QuickbooksSink):
    name = "Items"
    alias_name = "items"

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
            invoice_item = json.loads(record.get("invoiceItem"))
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
    alias_name = "creditnotes"

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
    alias_name = "paymentmethod"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        payment_methods = payment_method_from_unified(record)
        entry = ["PaymentMethod", payment_methods, "create"]

        context["records"].append(entry)


class PaymentTermSink(QuickbooksSink):
    name = "PaymentTerm"
    alias_name = "paymentterm"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        payment_terms = payment_term_from_unified(record)
        entry = ["Term", payment_terms, "create"]

        context["records"].append(entry)


class TaxRateSink(QuickbooksSink):
    name = "TaxRate"
    alias_name = "taxrate"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        tax_rates = tax_rate_from_unified(record)
        entry = ["TaxService", tax_rates, "create"]

        context["records"].append(entry)


class DepartmentSink(QuickbooksSink):
    name = "Department"
    alias_name = "department"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        departments = department_from_unified(record)
        entry = ["Department", departments, "create"]

        context["records"].append(entry)


class JournalEntrySink(QuickbooksSink):
    name = "JournalEntries"
    alias_name = "journalentries"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        # Get the journal entry id
        je_id = record["id"]

        line_items = []

        # Create line items
        for row in record["lines"]:
            # Create journal entry line detail
            je_detail = {"PostingType": row["postingType"]}

            # Get the Quickbooks Account Ref
            acct_num = str(row["accountNumber"])
            acct_name = row["accountName"]
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

            # Create the line item
            line_items.append(
                {
                    "Description": row["description"],
                    "Amount": row["amount"],
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
    alias_name = "bills"

    def process_record(self, record: dict, context: dict) -> None:
        # Bill id
        bill_id = record.get("id")
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

        if skip_vendor == True:
            self.logger.error(f"A valid vendor is required for creating bill. No match found for {record.get('vendorName')}. Skipping...")
            return

        if vendor is not None:
            entry["VendorRef"] = {"value": vendor["Id"]}

        # Create line items
        for row in record["lineItems"]:
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
                    f"Account and product is missing on Journal Entry {bill_id}! Name={acct_name} \n Skipping..."
                )
                return

            # Create the line item
            line_items.append(
                {
                    "Amount": row["totalPrice"],
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

        entry = ["Bill", entry, "create"]

        context["records"].append(entry)


class DepositsSink(QuickbooksSink):
    name = "Deposits"
    alias_name = "deposits"

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