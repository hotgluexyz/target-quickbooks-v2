"""
Functions to mapp from Hotglue's Unified Schema to the quickbooks' Schema  
"""
import json
import logging
from datetime import datetime


class EntityNotFoundException(Exception):
    pass


def jsonable_list_objs(obj):
    if isinstance(obj, str):
        obj = json.loads(obj)
    if isinstance(obj, dict):
        return [obj]
    return obj


def evalable_list_objs(obj):
    if isinstance(obj, str):
        obj = eval(obj)
    if isinstance(obj, dict):
        return [obj]
    return obj
    

def lookup_entity(record, id_field, name_field, entity, entity_list, required):
    entity_id = None
    if record.get(id_field) in [v["Id"] for v in entity_list.values()]:
        entity_id = record.get(id_field)
    elif record.get(name_field):
        entity_id = entity_list.get(record.get(name_field), {}).get("Id")
    if not entity_id and required:
        raise EntityNotFoundException(f"Could not find {entity} in Quickbooks matching Id= {id_field} or Name={name_field}")
    return entity_id


def lookup_entity_tuples(record, id_field_tuples, name_field_tuples, entity, entity_list, required):
    for ref_id_field, lookup_id_field in id_field_tuples:
        entity_id = record.get(lookup_id_field)
        if entity_id and entity_id in [v[ref_id_field] for v in entity_list.values() if v.get(ref_id_field)]:
            return entity_id

    for ref_id_field, lookup_name_field in name_field_tuples:
        entity_name = record.get(lookup_name_field)
        if entity_name:
            entity_id = entity_list.get(entity_name, {}).get(ref_id_field)
            if entity_id:
                return entity_id
    if required:
        id_tuple_str = ';'.join([f'{ref}={lkup}' for ref, lkup in  id_field_tuples])
        name_tuple_str = ';'.join([f'{ref}={lkup}' for ref, lkup in  name_field_tuples])
        raise EntityNotFoundException(f"Could not find {entity} in Quickbooks matching {id_tuple_str} or {name_tuple_str}")
    return None


def customer_from_unified(record):
    mapp = {
        "customerName": "CompanyName",
        "contactName": "DisplayName",
        "firstName": "GivenName",
        "middleName": "MiddleName",
        "lastName": "FamilyName",
        "suffix": "Suffix",
        "title": "Title",
        "active": "Active",
        "notes": "Notes",
        "checkName": "PrintOnCheckName",
        "balance": "Balance",
        "balanceDate": "OpenBalanceDate",
        "taxable": "Taxable",
    }

    customer = dict(
        (mapp[key], value) for (key, value) in record.items() if key in mapp.keys()
    )

    customer["PrimaryEmailAddr"] = {"Address": record.get("emailAddress", "")}

    if record.get("website"):
        customer["WebAddr"] = {"URI": record["website"]}
    if record.get("balanceDate"):
        balance_date = datetime.strptime(record["balanceDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
        customer["OpenBalanceDate"] = balance_date.strftime("%Y-%m-%d")

    # Get Parent
    if record.get("parentReference"):
        parent = record["parentReference"]
        # Set subcustomer
        customer["Job"] = True
        customer["ParentRef"] = {"value": parent["id"], "name": parent["name"]}

    phone_numbers = record.get("phoneNumbers")

    if phone_numbers:
        phone_numbers = evalable_list_objs(phone_numbers)

        fax_number = next((x for x in phone_numbers if x.get("type") == "fax"), None)
        if fax_number:
            customer["Fax"] = {"FreeFormNumber": fax_number["number"]}

        mobile_number = next(
            (x for x in phone_numbers if x.get("type") == "mobile"), None
        )
        if mobile_number:
            customer["Mobile"] = {"FreeFormNumber": mobile_number["number"]}

        primary_number = next(
            (x for x in phone_numbers if x.get("type") == "primary"), None
        )
        if primary_number:
            customer["PrimaryPhone"] = {"FreeFormNumber": primary_number["number"]}

        alternate_number = next(
            (x for x in phone_numbers if x.get("type") == "alternate"), None
        )
        if alternate_number:
            customer["AlternatePhone"] = {"FreeFormNumber": alternate_number["number"]}

    addresses = record.get("addresses")

    if addresses:
        addresses = evalable_list_objs(addresses)

        # TODO: Addresses should use type mapping for shipping/billing like we do for phone numbers above
        customer["BillAddr"] = {
            "Line1": addresses[0].get("line1"),
            "Line2": addresses[0].get("line2"),
            "Line3": addresses[0].get("line3"),
            "City": addresses[0].get("city"),
            "CountrySubDivisionCode": addresses[0].get("state"),
            "PostalCode": addresses[0].get("postalCode"),
            "Country": addresses[0].get("country"),
        }

        if len(addresses) > 1:
            customer["ShipAddr"] = {
                "Id": addresses[1].get("id"),
                "Line1": addresses[1].get("line1"),
                "Line2": addresses[1].get("line2"),
                "Line3": addresses[1].get("line3"),
                "City": addresses[1].get("city"),
                "CountrySubDivisionCode": addresses[1].get("state"),
                "PostalCode": addresses[1].get("postalCode"),
                "Country": addresses[1].get("country"),
            }

    return customer


def vendor_from_unified(record, tax_codes):
    vendor = {}

    if record.get("emailAddress"):
        vendor["PrimaryEmailAddr"] = {
            "Address": record.get("emailAddress")
        }

    phone_numbers = record.get("phoneNumbers")

    if phone_numbers:
        phone_numbers = evalable_list_objs(phone_numbers)

        mobile_number = next(
            (x for x in phone_numbers if x.get("type") == "mobile"), None
        )

        if mobile_number:
            vendor["Mobile"] = {"FreeFormNumber": mobile_number["number"]}

        primary_number = next(
            (x for x in phone_numbers if x.get("type") == "primary"), None
        )

        if primary_number:
            vendor["PrimaryPhone"] = {"FreeFormNumber": primary_number["number"]}
        else:
            primary_number = next(
                (x for x in phone_numbers if x.get("type") == "phone1"), None
            )

            if primary_number:
                vendor["PrimaryPhone"] = {"FreeFormNumber": primary_number["number"]}
            else:
                vendor["PrimaryPhone"] = {"FreeFormNumber": phone_numbers[0]["number"]}

    addresses = record.get("addresses")

    if addresses:
        addresses = evalable_list_objs(addresses)

        # TODO: Addresses should use type mapping for shipping/billing like we do for phone numbers above

        vendor["BillAddr"] = {
            "Line1": addresses[0].get("line1"),
            "Line2": addresses[0].get("line2"),
            "Line3": addresses[0].get("line3"),
            "City": addresses[0].get("city"),
            "CountrySubDivisionCode": addresses[0].get("state"),
            "PostalCode": addresses[0].get("postalCode"),
            "Country": addresses[0].get("country"),
        }

    if record.get("vendorName"):
        vendor["DisplayName"] = record.get("vendorName")
        vendor["CompanyName"] = record.get("vendorName")
        vendor["PrintOnCheckName"] = record.get("vendorName")

    if record.get("contactName"):
        contact_name = record.get("contactName")

        if len(contact_name.split()) > 1:
            given_name, *family_name = contact_name.split()

            if isinstance(family_name, list):
                family_name = " ".join(family_name)
            else:
                given_name, family_name = contact_name, None

            vendor["GivenName"] = given_name
            vendor["FamilyName"] = family_name

    # TODO: I am pretty sure the below is wrong, so I've commented it out for now.
    # if record.get("bankAccounts"):
    #     bank_accounts = evalable_list_objs(record.get("bankAccounts"))

    #     tax_code_id = lookup_entity(record, None, "taxCode", "TaxCode", tax_codes, False)

    #     if len(bank_accounts):
    #         vendor["TaxIdentifier"] = tax_code_id

    #     vendor["AcctNum"] = bank_accounts[0].get("accountNumber")

    return vendor

def item_from_unified(record, tax_codes, categories):
    mapp = {
        "name": "Name",
        "active": "Active",
        "type": "Type",
        "fullyQualifiedName": "FullyQualifiedName",
        "sku": "Sku",
        "reorderPoint": "ReorderPoint",
        "taxable": "Taxable",
        "invStartDate": "InvStartDate",
    }

    categories = {
        v["Name"]: v["Id"] for v in categories.values() if v["Type"] == "Category"
    }

    item = dict(
        (mapp[key], value) for (key, value) in record.items() if key in mapp.keys()
    )

    if record.get("isBillItem", False) and record.get("billItem"):
        billItem = record["billItem"]
        if isinstance(billItem, str):
            billItem = eval(billItem)

        item["PurchaseCost"] = billItem.get("unitPrice")
        item["PurchaseDesc"] = billItem.get("description")
        item["Description"] = billItem.get("description")
        item["ExpenseAccountNum"] = billItem.get("accountId")

    if record.get("isInvoiceItem", False) and record.get("invoiceItem"):
        invoiceItem = record["invoiceItem"]
        if isinstance(invoiceItem, str):
            invoiceItem = eval(invoiceItem)

        item["Description"] = invoiceItem.get("description")
        item["IncomeAccountNum"] = invoiceItem.get("accountId")
        item["UnitPrice"] = invoiceItem.get("unitPrice")
    else:
        invoiceItem = {}

    # Hardcoding "QtyOnHand" = 0 if "type" == "Inventory"
    if item["Type"] == "Inventory":
        today = datetime.now()
        item["InvStartDate"] = invoiceItem.get("startDate") or today.strftime(
            "%Y-%m-%d"
        )
        item["TrackQtyOnHand"] = True
        if record.get("quantityOnHand"):
            item["QtyOnHand"] = record.get("quantityOnHand")

    if record.get("taxCode"):
        item["SalesTaxCodeRef"] = {
            "value": tax_codes[record.get("taxCode")]["Id"],
            "name": record.get("taxCode"),
        }

    if record.get("category"):
        if record.get("category") in categories:
            item["SubItem"] = True
            item["ParentRef"] = {
                "value": categories[record.get("category")],
                "name": record.get("category"),
            }

    return item


def invoice_line(record, items, products, tax_codes=None):
    lines = []
    items = jsonable_list_objs(items)

    total_discount = 0

    for item in items:
        if not item.get("productName"):
            raise Exception(f"productName is empty, please review the payload")
        product = products.get(item.get("productName"))
        if not product:
            raise Exception(f"{item.get('productName')} is not a valid product in this Quickbooks company.")
        product_id = product["Id"]

        item_line_detail = {
            "ItemRef": {"value": product_id},
            "Qty": item.get("quantity"),
            "UnitPrice": item.get("unitPrice"),
            "DiscountAmt": item.get("discountAmount"),
        }

        # if item.get("shippingAmount"):
        #     item_line_detail['ItemRef'] = {
        #         "value" : "SHIPPING_ITEM_ID",
        #         "name" : item.get("shippingAmount")
        #     }

        if item.get("serviceDate"):
            item_line_detail["ServiceDate"] = item.get("serviceDate")

        if tax_codes and item.get("taxCode") is not None:
            item_line_detail.update({"TaxCodeRef": {"value": item.get("taxCode")}})

        # Check if this line item is the shipping amount.
        if item.get("shipping"):
            item_line_detail["ItemRef"] = {"value": "SHIPPING_ITEM_ID"}

        line_item = {
            "DetailType": "SalesItemLineDetail",
            "Amount": item.get("totalPrice"),
            "SalesItemLineDetail": item_line_detail,
            "Description": item.get("description"),
        }

        if product.get("TrackQtyOnHand"):
            produdct_qty = product.get("QtyOnHand")
            if not (isinstance(produdct_qty, int) or isinstance(produdct_qty, float)):
                logging.info(
                    f"Invalid QtyOnHand for Product: {item.get('productName')}. QtyOnHand: {produdct_qty}"
                )
                line_item = None
            if produdct_qty < 1:
                logging.info(
                    f"No quantity available for Product: {item.get('productName')}"
                )
                line_item = None

        if line_item:
            lines.append(line_item)

        if item.get("discountAmount"):
            total_discount += item.get("discountAmount")

    discount_line = {
        "DetailType": "DiscountLineDetail",
        "Amount": None,
        "Description": "Less discount",
        "DiscountLineDetail": {"PercentBased": False},
    }

    if record.get("totalDiscount"):
        discount_line["Amount"] = record.get("totalDiscount")
    elif total_discount:
        discount_line["Amount"] = total_discount
    else:
        discount_line["Amount"] = 0
    lines.append(discount_line)

    return lines


def invoice_from_unified(record, customers, products, tax_codes, sales_terms):
    # Get customer
    customer_id = lookup_entity(record, "customerId", "customerName", "Customer", customers, True)
    
    invoice_lines = invoice_line(record, record.get("lineItems"), products, tax_codes)

    invoice = {
        "Line": invoice_lines,
        "CustomerRef": {"value": customer_id},
        "TotalAmt": record.get("totalAmount"),
        "TxnDate": record.get("issueDate"),
        "TrackingNum": record.get("trackingNumber"),
        "EmailStatus": record.get("emailStatus"),
        "DocNumber": record.get("invoiceNumber"),
        "PrivateNote": record.get("invoiceMemo"),
        "Deposit": record.get("deposit"),
        "TxnTaxDetail": {
            "TotalTax": record.get("taxAmount"),
        },
        "ApplyTaxAfterDiscount": record.get("applyTaxAfterDiscount", True),
    }

    # TODO: Is this field required?
    if record.get("dueDate"):
        record["DueDate"] = record.get("dueDate").split("T")[0]

    if record.get("shipDate"):
        invoice["ShipDate"] = record.get("shipDate")

    if record.get("taxAmount"):
        invoice["TotalTax"] = record.get("taxAmount")

    tax_code_id = lookup_entity(record, None, "taxCode", "TaxCode", tax_codes, False)
    if tax_code_id:
        invoice["TxnTaxDetail"] = {
            "TxnTaxCodeRef": {"value": tax_code_id},
        }

    if record.get("customerMemo"):
        invoice["CustomerMemo"] = {"value": record.get("customerMemo")}

    if record.get("billEmail"):
        # Set needs to status here because BillEmail is required if this parameter is set.
        invoice["EmailStatus"] = "NeedToSend"
        invoice["BillEmail"] = {"Address": record.get("billEmail")}

    if record.get("billEmailCc"):
        invoice["BillEmailCc"] = {"Address": record.get("billEmailCc")}

    if record.get("billEmailBcc"):
        invoice["BillEmailBcc"] = {"Address": record.get("billEmailBcc")}

    # if record.get("shipMethod"):
    #     invoice["ShipMethodRef"] = {
    #         "value" : record.get("id"),
    #         "name" : record.get("name")
    #     }

    if record.get("salesTerm"):
        sales_term_id = lookup_entity(record, None, "salesTerm", "SalesTerm", sales_terms, False)
        if sales_term_id:
            invoice["SalesTermRef"] = {
                "value": sales_term_id,
                "name": record.get("salesTerm"),
            }

    addresses = record.get("addresses")

    if addresses:
        addresses = evalable_list_objs(addresses)

        invoice["BillAddr"] = {
            "Line1": addresses[0].get("line1"),
            "Line2": addresses[0].get("line2"),
            "Line3": addresses[0].get("line3"),
            "City": addresses[0].get("city"),
            "CountrySubDivisionCode": addresses[0].get("state"),
            "PostalCode": addresses[0].get("postalCode"),
            "Country": addresses[0].get("country"),
        }

        if len(addresses) > 1:
            invoice["ShipAddr"] = {
                "Id": addresses[1].get("id"),
                "Line1": addresses[1].get("line1"),
                "Line2": addresses[1].get("line2"),
                "Line3": addresses[1].get("line3"),
                "City": addresses[1].get("city"),
                "CountrySubDivisionCode": addresses[1].get("state"),
                "PostalCode": addresses[1].get("postalCode"),
                "Country": addresses[1].get("country"),
            }

    if not invoice_lines:
        if record.get("id"):
            raise Exception(f"No Invoice Lines for Invoice id: {record['id']}")
        elif record.get("invoiceNumber"):
            raise Exception(
                f"No Invoice Lines for Invoice Number: {record['invoiceNumber']}"
            )
        return []

    return invoice


def sales_receipt_line(record, items, products, tax_codes=None):
    lines = []
    items = jsonable_list_objs(items)

    total_discount = 0

    for item in items:
        product = None

        product_id = lookup_entity_tuples(
            record,
            [
                ("Id", "productId"),
                ("Sku", "productId"),
                ("Sku", "sku"),
            ],
            [("Id", "productName")],
            "Product",
            products,
            True
        )

        item_line_detail = {
            "ItemRef": {"value": product_id},
            "Qty": item.get("quantity"),
            "UnitPrice": item.get("unitPrice"),
            "DiscountAmt": item.get("discountAmount"),
        }

        if tax_codes and item.get("taxCode") is not None:
            item_line_detail.update({"TaxCodeRef": {"value": item.get("taxCode")}})

        if item.get("serviceDate"):
            item_line_detail["ServiceDate"] = item.get("serviceDate")

        if tax_codes and item.get("taxCode") is not None:
            item_line_detail.update({"TaxCodeRef": {"value": item.get("taxCode")}})

        line_item = {
            "DetailType": "SalesItemLineDetail",
            "Amount": item.get("totalPrice"),
            "SalesItemLineDetail": item_line_detail,
            "Description": item.get("description"),
        }

        if product["TrackQtyOnHand"]:
            if product["QtyOnHand"] < 1:
                logging.info(
                    f"No quantity available for Product: {item.get('productName')}"
                )
                line_item = None

        if line_item:
            lines.append(line_item)

        if item.get("discountAmount"):
            total_discount += item.get("discountAmount")
    
    if record.get("shippingAmount"):
        shipping_line = {
            "DetailType": "SalesItemLineDetail",
            "Amount": record.get("shippingAmount"),
            "SalesItemLineDetail": {
                "ItemRef": {
                    "value" : "SHIPPING_ITEM_ID",
                    "name" : record.get("shippingAmount"),
                },
                "TaxCodeRef": {"value": "TAX"}
            },
        }
        lines.append(shipping_line)

    discount_line = {
        "DetailType": "DiscountLineDetail",
        "Amount": None,
        "Description": "Less discount",
        "DiscountLineDetail": {"PercentBased": False},
    }

    if record.get("totalDiscount"):
        discount_line["Amount"] = record.get("totalDiscount")
    elif total_discount:
        discount_line["Amount"] = total_discount
    else:
        discount_line["Amount"] = 0
    lines.append(discount_line)

    return lines


def sales_receipt_from_unified(record, customers, products, tax_codes):
    customer_name = record.get("customerName",record.get("customer_name"))
    customer_id = None

    if customer_name and customers.get(customer_name):
        customer_id = customers[customer_name]["Id"]
    else:
        logging.warn(f"Could not find matching customer for {customer_name}")

    sales_lines = sales_receipt_line(
        record, record.get("lineItems"), products, tax_codes
    )

    sales_receipt = {
        "Line": sales_lines,
        "TotalAmt": record.get("totalAmount"),
        "TxnDate": record.get("issueDate"),
        "DocNumber": record.get("salesNumber"),
        "TxnTaxDetail": {
            "TotalTax": record.get("taxAmount"),
        },
        "ApplyTaxAfterDiscount": record.get("applyTaxAfterDiscount", True),
    }

    if customer_id:
        sales_receipt["CustomerRef"] = {"value": customer_id}

    if record.get("taxCode"):
        sales_receipt["TxnTaxDetail"] = {
            "TxnTaxCodeRef": {"value": tax_codes[record.get("taxCode")]["Id"]},
        }

    if record.get("billEmail"):
        # Set needs to status here because BillEmail is required if this parameter is set.
        sales_receipt["EmailStatus"] = "NeedToSend"
        sales_receipt["BillEmail"] = {"Address": record.get("billEmail")}

    if record.get("billAddress"):
        billAddr = record.get("billAddress")
        sales_receipt["BillAddr"] = {
            "Line1": billAddr.get("line1"),
            "Line2": billAddr.get("line2"),
            "Line3": billAddr.get("line3"),
            "City": billAddr.get("city"),
            "CountrySubDivisionCode": billAddr.get("state"),
            "PostalCode": billAddr.get("postalCode"),
            "Country": billAddr.get("country"),
        }
    if record.get("shipAddress"):
        shipAddr = record.get("shipAddress")
        sales_receipt["ShipAddr"] = {
            "Line1": shipAddr.get("line1"),
            "Line2": shipAddr.get("line2"),
            "Line3": shipAddr.get("line3"),
            "City": shipAddr.get("city"),
            "CountrySubDivisionCode": shipAddr.get("state"),
            "PostalCode": shipAddr.get("postalCode"),
            "Country": shipAddr.get("country"),
        }

    if not sales_lines:
        if record.get("id"):
            raise Exception(f"No Invoice Lines for Invoice id: {record['id']}")
        elif record.get("invoiceNumber"):
            raise Exception(
                f"No Invoice Lines for Invoice Number: {record['invoiceNumber']}"
            )
        return []
    
    if "payment_id" in record:
        sales_receipt["PaymentRefNum"] = record.get("payment_id")

    return sales_receipt


def credit_line(items, products, tax_codes=None):
    lines = []
    items = jsonable_list_objs(items)

    for item in items:
        if not item.get("productName"):
            raise Exception(f"productName is empty, please review the payload")
        product = products.get(item.get("productName"))
        if not product:
            raise Exception(f"{item.get('productName')} is not a valid product in this Quickbooks company.")
        product_id = product["Id"]

        item_line_detail = {
            "ItemRef": {"value": product_id},
        }

        if product.get("QtyOnHand"):
            item_line_detail.update({"Qty": item.get("quantity")})

        line_item = {
            "DetailType": "SalesItemLineDetail",
            "Amount": item.get("totalAmount"),
            "SalesItemLineDetail": item_line_detail,
        }

        if line_item:
            lines.append(line_item)

    return lines


def creditnote_from_unified(record, customers, products, tax_codes):
    customer_id = customers[record.get("customerRef").get("customerName")]["Id"]

    invoice_lines = credit_line(record.get("lineItems"), products)
    # invoice_lines = invoice_line(record.get("lineItems"), products)

    creditnote = {"Line": invoice_lines, "CustomerRef": {"value": customer_id}}
    return creditnote


def payment_method_from_unified(record):
    payment_method = record

    return payment_method


def payment_term_from_unified(record):
    payment_term = record

    return payment_term


def tax_rate_from_unified(record):
    tax_rate = record

    return tax_rate


def department_from_unified(record):
    department = record

    return department

def deposit_from_unified(record, entity):
    ref_accounts = entity.accounts
    ref_classes = entity.classes
    ref_customers = entity.customers

    qb_deposit = {
        "Line": [], 
        "DepositToAccountRef": {
            "name": record.get("accountName"), 
            "value": ref_accounts.get(record.get("accountName"), {}).get("Id") if not record.get("accountId") else record.get("accountId"),
        },
        "TxnDate": record.get("issueDate"),
    }

    qb_deposit["CurrencyRef"] = {
        "value": record.get("currency"),
    }

    for line_item in record.get("lineItems", []):
        content = {
            "DetailType": "DepositLineDetail",
            "Amount": line_item.get("amount"),
            "DepositLineDetail": {
                "AccountRef": {
                    "name": line_item.get("accountName"),
                    "value": entity.accounts_name.get(line_item.get("accountName"), entity.accounts.get(line_item["accountName"], {})).get("Id"),
                },
                "Entity": {
                    # TODO: this could be none value? or is better to not have Entity in that case?
                    "name": line_item.get("customerName"),
                    "value": ref_customers.get(line_item.get("customerName"), {}).get("Id")
                }
            }
        }
        if ref_classes.get(line_item.get("className"), {}).get("Id", False):
            content["DepositLineDetail"]["ClassRef"] = {
                "name": line_item.get("className"),
                "value": ref_classes.get(line_item.get("className"), {}).get("Id") if not line_item.get("classId") else line_item.get("classId")
            }
            
        qb_deposit["Line"].append(content)

    return qb_deposit