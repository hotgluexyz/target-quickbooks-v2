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
    department_from_unified
)


class InvoiceSink(QuickbooksSink):
    name = "Invoices"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        invoice = invoice_from_unified(
            record,
            self.customers,
            self.items,
            self.tax_codes,
            self.sales_terms
        )
        if record.get("id"):
            invoice_details = self.get_entities("Invoice", check_active=False, fallback_key="Id" ,where_filter=f" id ='{record.get('id')}'")
            if str(record.get("id")) in invoice_details:
                invoice.update({"Id":record.get("id"),"sparse":True,"SyncToken": invoice_details[record.get("id")]["SyncToken"]})
                entry = ["Invoice", invoice, "update"]
            else:
                print(f"Invoice {record.get('id')} not found. Skipping...")  
                return
        else:
            entry = ["Invoice", invoice, "create"]    

            self.logger.info(json.dumps(entry))

        context["records"].append(entry)


class CustomerSink(QuickbooksSink):
    name = "Customers"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        customer = customer_from_unified(record)

        if record.get("term") and record["term"] in self.terms:
            term = self.terms[record['term']]
            customer["SalesTermRef"] = {"value": term['Id']}

        #Get Customer Type
        if record.get("customerType") and record.get("customerType") in self.customer_type:
            customer_type = self.customer_type[record["customerType"]]
            customer["CustomerTypeRef"] = {"value": customer_type["Id"]}

        #Get Tax Code
        if record.get("taxCode") and record.get("taxCode") in self.tax_codes:
            tax_code = self.tax_codes[record['taxCode']]
            customer["DefaultTaxCodeRef"] = {"value": tax_code['Id'], "name": tax_code['Name']}

        #Get Payment Method
        if record.get("paymentMethod") and record.get("paymentMethod") in self.payment_methods:
            pm = self.payment_methods[record['paymentMethod']]
            customer["PaymentMethodRef"] = {"value": pm['Id'], "name": pm['Name']}

        if customer["DisplayName"] in self.customers:
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

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        item = item_from_unified(record, self.tax_codes)

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

        if item["Name"] in self.items:
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

        payment_terms =  payment_term_from_unified(record)
        entry = ["Term", payment_terms, "create"]

        context["records"].append(entry)


class TaxRateSink(QuickbooksSink):
    name = "TaxRate"

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        tax_rates =  tax_rate_from_unified(record)
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
        for row in record["lines"]:
            # Create journal entry line detail
            je_detail = {"PostingType": row["postingType"]}

            # Get the Quickbooks Account Ref
            acct_num = str(row["accountNumber"])
            acct_name = row["accountName"]
            acct_ref = self.accounts.get(acct_num, self.accounts.get(acct_name, {})).get("Id")

            if acct_ref is not None:
                je_detail["AccountRef"] = {"value": acct_ref}
            else:
                errored = True
                self.logger.error(f"Account is missing on Journal Entry {je_id}! Name={acct_name} No={acct_num} \n Skipping...")
                return

            # Get the Quickbooks Class Ref
            class_name = row.get("className")
            class_ref = self.classes.get(class_name, {}).get("Id")

            if class_ref is not None:
                je_detail["ClassRef"] = {"value": class_ref}
            else:
                self.logger.warning(f"Class is missing on Journal Entry {je_id}! Name={class_name}")

            # Get the Quickbooks Customer Ref
            customer_name = row.get("customerName")
            customer_ref = self.customers.get(customer_name, {}).get("Id")

            if customer_ref is not None:
                je_detail["Entity"] = {"EntityRef": {"value": customer_ref}, "Type": "Customer"}
            else:
                self.logger.warning(f"Customer is missing on Journal Entry {je_id}! Name={customer_name}")

            # Get the Quickbooks Vendor Ref
            vendor_name = row.get("vendorName")
            vendor_ref = self.vendors.get(vendor_name, {}).get("Id")

            if vendor_ref is not None:
                je_detail["Entity"] = {"EntityRef": {"value": vendor_ref},"Type": "Vendor"}
            else:
                self.logger.warning(f"Vendor is missing on Journal Entry {je_id}! Name={vendor_name}")

            # Create the line item
            line_items.append({
                "Description": row["description"],
                "Amount": row["amount"],
                "DetailType": "JournalEntryLineDetail",
                "JournalEntryLineDetail": je_detail
            })

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
        entry = {}
        vendor = None
        skip_vendor = True
        line_items = []
        if record.get('department'):
            departments = self.get_departments()
            if record['department'] in departments:
                department = departments[record['department']]
                entry['DepartmentRef'] = {
                    "value":department['Id'],
                    "name":department['Name'],
                }
        if "vendorName" in record:
            if record["vendorName"] in self.vendors:
                vendor = self.vendors[record["vendorName"]]
                skip_vendor = False
            else:
                skip_vendor = True

        if skip_vendor == True:
            print("A valid vendor is required for creating bill. Skipping...")
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
            #Check if product name is provided
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
