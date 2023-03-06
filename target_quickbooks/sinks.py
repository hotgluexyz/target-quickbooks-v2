from os import environ
import requests
import json
from singer_sdk.sinks import BatchSink
from datetime import datetime
from intuitlib.client import AuthClient
from intuitlib.enums import Scopes
from singer_sdk.plugin_base import PluginBase
from typing import Any, Dict, List, Mapping, Optional, Union
import re

from target_quickbooks.mapper import (
    customer_from_unified,
    item_from_unified,
    invoice_from_unified,
    creditnote_from_unified,
)


class QuickBooksSink(BatchSink):
    """QuickBooks target sink class."""

    max_size = 30  # Max records to write in one batch

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        # Save config for refresh_token saving
        self.config_file = target.config_file

        # Instantiate Client
        self.instantiate_client()

        # Get reference data
        self.get_reference_data()

    def instantiate_client(self):
        self.last_refreshed = None
        self.access_token = self.config.get("access_token")
        self.refresh_token = self.config.get("refresh_token")

        client_id = self.config.get("client_id")
        client_secret = self.config.get("client_secret")
        redirect_uri = self.config.get("redirect_uri")

        if self.config.get("is_sandbox"):
            environment = "sandbox"
        else:
            environment = "production"

        self.auth_client = AuthClient(
            client_id, client_secret, redirect_uri, environment
        )

        if not self.is_token_valid():
            self.update_access_token()

        realm = self.config.get("realmId")

        if self.config.get("is_sandbox"):
            self.base_url = (
                f"https://sandbox-quickbooks.api.intuit.com/v3/company/{realm}"
            )
        else:
            self.base_url = f"https://quickbooks.api.intuit.com/v3/company/{realm}"

    def get_reference_data(self):
        self.accounts = self.get_entities("Account", key="AcctNum")
        self.accounts_name = self.get_entities("Account", key="Name")
        self.customers = self.get_entities("Customer", key="DisplayName")
        self.items = self.get_entities("Item", key="Name")
        self.classes = self.get_entities("Class")
        self.tax_codes = self.get_entities("TaxCode")
        self.vendors = self.get_entities("Vendor", key="DisplayName")
        self.terms = self.get_entities("Term", key="Name")
        self.customer_type = self.get_entities("CustomerType", key="Name")
        self.payment_methods = self.get_entities("PaymentMethod", key="Name")

    def update_access_token(self):
        self.auth_client.refresh(self.config.get("refresh_token"))
        self.access_token = self.auth_client.access_token
        self.refresh_token = self.auth_client.refresh_token
        self._config["refresh_token"] = self.refresh_token
        self._config["access_token"] = self.access_token
        self._config["last_update"] = round(datetime.now().timestamp())

        with open(self.config_file, "w") as outfile:
            json.dump(self._config, outfile, indent=4)

    def is_token_valid(self):
        last_update = self.config.get("last_update")
        if not last_update:
            return False
        if round(datetime.now().timestamp()) - last_update > 3300:  # 1h - 5min
            return False
        return True

    @property
    def authenticator(self):
        auth_credentials = {"Authorization": f"Bearer {self.access_token}"}
        return auth_credentials

    @property
    def get_url_params(self):
        params = {}
        params["minorversion"] = 40  # minorversion=40
        return params

    def start_batch(self, context: dict) -> None:
        if not self.is_token_valid():
            # If the token is invalid, refresh the access token
            self.update_access_token()

    def get_entities(
        self, entity_type, key="Name", fallback_key="Name", check_active=True , where_filter=None
    ):
        access_token = self.access_token
        offset = 0
        max = 100
        entities = {}

        while True:
            query = f"select * from {entity_type}"
            if check_active:
                query = query + " where Active=true"

            if where_filter and check_active==False:
               query = query + f" where {where_filter}" 
                

            query = query + f" STARTPOSITION {offset} MAXRESULTS {max}"
            url = f"{self.base_url}/query?query={query}&minorversion=40"

            self.logger.info(f"Fetch {entity_type}; url={url}; query {query}")
            
            r = requests.get(
                url,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {access_token}",
                },
            )


            response = r.json()

            # Establish number of records returned.
            count = response["QueryResponse"].get("maxResults")

            # No results - exit loop.
            if not count or count == 0:
                break

            # Parse the results
            records = response["QueryResponse"][entity_type]

            if not records:
                records = []

            # Append the results
            for record in records:
                entity_key = record.get(key, record.get(fallback_key))
                # Ignore None keys
                if entity_key is None:
                    self.logger.warning(f"Failed to parse record f{json.dumps(record)}")
                    continue

                entities[entity_key] = record

            # We're done - exit loop
            if count < max:
                break

            offset += max

        self.logger.debug(f"[get_entities]: Found {len(entities)} {entity_type}.")

        return entities

    def search_reference_data(self, reference_data, key, value):
        return_data = {}
        for data in reference_data:
            if key in data:
                if data[key] == value:
                    return data
        return return_data

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = []

        line_items = []

        if self.stream_name == "Customers":

            customer = customer_from_unified(record)
            if "term" in record:
                if record['term'] in self.terms:
                    term = self.terms[record['term']]
                    customer["SalesTermRef"] = {
                        "value": term['Id']
                    }
            #Get Customer Type
            if record.get("customerType") :
                if record.get("customerType") in self.customer_type:
                    customer_type = self.customer_type[record["customerType"]]
                    customer["CustomerTypeRef"] = {
                        "value": customer_type["Id"],
                        
                    }    
            #Get Tax Code        
            if record.get("taxCode") :
                if record.get("taxCode") in self.tax_codes:
                    tax_code = self.tax_codes[record['taxCode']]
                    customer["DefaultTaxCodeRef"] = {
                        "value": tax_code['Id'],
                        "name": tax_code['Name']
                }    
                    
            #Get Payment Method        
            if record.get("paymentMethod") :
                if record.get("paymentMethod") in self.payment_methods:
                    pm = self.payment_methods[record['paymentMethod']]
                    customer["PaymentMethodRef"] = {
                        "value": pm['Id'],
                        "name": pm['Name']
                }    
            if customer["DisplayName"] in self.customers:
                old_customer = self.customers[customer["DisplayName"]]
                customer["Id"] = old_customer["Id"]
                customer["SyncToken"] = old_customer["SyncToken"]
                customer["sparse"] = True
                entry = ["Customer", customer, "update"]
            else:
                entry = ["Customer", customer, "create"]

        elif self.stream_name == "Invoices":

            invoice = invoice_from_unified(
                record, self.customers, self.items, self.tax_codes
            )
            if record.get("id"):
                invoice_details = self.get_entities("Invoice", check_active=False, fallback_key="Id" ,where_filter=f" id ='{record.get('id')}'")
                if record.get("id") in invoice_details:
                    invoice.update({"Id":record.get("id"),"sparse":True,"SyncToken": invoice_details[record.get("id")]["SyncToken"]})
                    entry = ["Invoice", invoice, "update"]
                else:
                    print(f"Invoice {record.get('id')} not found. Skipping...")    
            else:
                entry = ["Invoice", invoice, "create"]    

            self.logger.info(json.dumps(entry))

        elif self.stream_name == "Items":

            item = item_from_unified(record)

            # Have to include AssetAccountRef if we're creating an Inventory item
            if item.get("Type") == "Inventory":
                # TODO: Below is hardcoded
                item["AssetAccountRef"] = {
                    "value": self.accounts["Inventory Asset"]["Id"]
                }

            # Convert account num -> accountRef
            income_account_num = item.pop("IncomeAccountNum", None)
            income_account = self.accounts.get(income_account_num) if self.accounts.get(income_account_num) else self.accounts_name.get(income_account_num)

            if income_account:
                item["IncomeAccountRef"] = {
                    "value": income_account["Id"]
                }

            expense_account_num = item.pop("ExpenseAccountNum", None)
            expense_account = self.accounts.get(expense_account_num) if self.accounts.get(expense_account_num) else self.accounts_name.get(expense_account_num)

            if expense_account:
                item["ExpenseAccountRef"] = {
                    "value": expense_account["Id"]
                }

            if item["Name"] in self.items:
                old_item = self.items[item["Name"]]
                item["Id"] = old_item["Id"]
                item["SyncToken"] = old_item["SyncToken"]
                entry = ["Item", item, "update"]
            else:
                entry = ["Item", item, "create"]

        elif self.stream_name == "CreditNotes":

            creditnotes = creditnote_from_unified(
                record, self.customers, self.items, self.tax_codes
            )

            entry = ["CreditMemo", creditnotes, "create"]

        elif self.stream_name == "JournalEntries":

            # Get the journal entry id
            je_id = record["id"]

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
                    errored = True
                    self.logger.error(
                        f"Account is missing on Journal Entry {je_id}! Name={acct_name} No={acct_num} \n Skipping..."
                    )
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
                customer_name = row["customerName"]
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

        elif self.stream_name == "Bills":
            # Bill id
            bill_id = record.get("id")
            entry = {}
            vendor = None
            skip_vendor = True
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
                        "Description": record.get("description"),
                        "Amount": record.get("totalAmount"),
                    }
                )
            entry.update(
                {"Id": bill_id, "Line": line_items, "DueDate": record.get("dueDate")}
            )
            # Append the currency if provided
            if record.get("currency") is not None:
                entry["CurrencyRef"] = {"value": record["currency"]}
            entry = ["Bill", entry, "create"]

        context["records"].append(entry)

    def make_batch_request(self, url, batch_requests):
        access_token = self.access_token

        # Send the request
        r = requests.post(
            url,
            data=json.dumps({"BatchItemRequest": batch_requests}),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}",
            },
        )

        response = r.json()
        self.logger.info(f"DEBUG RESPONSE: {response}")
        if response.get("Fault") is not None:
            self.logger.error(response)

        return response.get("BatchItemResponse")

    def process_batch(self, context: dict) -> None:
        # Build endpoint url
        url = f"{self.base_url}/batch?minorversion=4"

        # Get the journals to post
        records = context.get("records")

        # Create the batch requests
        batch_requests = []

        for i, entity in enumerate(records):
            # entity[0] -> "Customer","JournalEntry", ...
            # entity[1] -> data
            # entity[2] -> "create" or "update"
            if entity[1]:
                batch_requests.append(
                    {"bId": f"bid{i}", "operation": entity[2], entity[0]: entity[1]}
                )

        if batch_requests:
            response_items = self.make_batch_request(url, batch_requests)
        else:
            response_items = []

        if not response_items:
            response_items = []

        posted_records = []
        failed = False

        for ri in response_items:
            if ri.get("Fault") is not None:
                m = re.search("[0-9]+$", ri.get("bId"))
                index = int(m.group(0))
                self.logger.error(
                    f"Failure creating entity error=[{json.dumps(ri)}] request=[{batch_requests[index]}]"
                )
                failed = True
            elif ri.get("JournalEntry") is not None:
                je = ri.get("JournalEntry")
                # Cache posted journal ids to delete them in event of failure
                posted_records.append(
                    {"Id": je.get("Id"), "SyncToken": je.get("SyncToken")}
                )
            elif ri.get("Customer") is not None:
                je = ri.get("Customer")
                # Cache posted customer ids to delete them in event of failure
                posted_records.append(
                    {"Id": je.get("Id"), "SyncToken": je.get("SyncToken")}
                )
            elif ri.get("Item") is not None:
                je = ri.get("Item")
                # Cache posted customer ids to delete them in event of failure
                posted_records.append(
                    {"Id": je.get("Id"), "SyncToken": je.get("SyncToken")}
                )
            elif ri.get("Invoice") is not None:
                je = ri.get("Invoice")
                # Cache posted customer ids to delete them in event of failure
                posted_records.append(
                    {"Id": je.get("Id"), "SyncToken": je.get("SyncToken")}
                )
            elif ri.get("CreditMemo") is not None:
                je = ri.get("CreditMemo")
                # Cache posted customer ids to delete them in event of failure
                posted_records.append(
                    {"Id": je.get("Id"), "SyncToken": je.get("SyncToken")}
                )
            elif ri.get("Bill") is not None:
                je = ri.get("Bill")
                # Cache posted Bill ids to delete them in event of failure
                posted_records.append(
                    {"Id": je.get("Id"), "SyncToken": je.get("SyncToken")}
                )

        if failed:
            batch_requests = []
            # In the event of failure, we need to delete the posted records
            for i, je in enumerate(posted_records):
                batch_requests.append(
                    {"bId": f"bid{i}", "operation": "delete", "JournalEntry": je}
                )

            # Do delete batch requests
            self.logger.info("Deleting any posted records entries...")
            response = self.make_batch_request(url, batch_requests)
            self.logger.debug(json.dumps(response))

            raise Exception("There was an error posting the records")

        pass
