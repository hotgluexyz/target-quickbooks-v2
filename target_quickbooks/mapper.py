"""
Functions to mapp from Hotglue's Unified Schema to the quickbooks' Schema  
"""
import json
import logging
import datetime


def customer_from_unified(record):

    mapp = {
        "customerName": "CompanyName",
        "contactName": "DisplayName",
        "firstName": "GivenName",
        "middleName": "MiddleName",
        "lastName": "FamilyName",
        "suffix": "Suffix",
        "title": "Title",
        "active": "Active"
    }

    customer = dict(
        (mapp[key], value) for (key, value) in record.items() if key in mapp.keys()
    )

    customer["PrimaryEmailAddr"] = {"Address": record.get("emailAddress", "")}

    if record.get("website"):
        customer["WebAddr"] = {
            "URI": record["website"]
        }

    phone_numbers = record.get("phoneNumbers")

    if phone_numbers:
        if isinstance(phone_numbers, str):
            phone_numbers = eval(phone_numbers)

        fax_number = next((x for x in phone_numbers if x.get('type') == "fax"), None)
        if fax_number:
            customer["Fax"] = {
                "FreeFormNumber": fax_number['number']
            }

        mobile_number = next((x for x in phone_numbers if x.get('type') == "mobile"), None)
        if mobile_number:
            customer["Mobile"] = {
                "FreeFormNumber": mobile_number['number']
            }

        primary_number = next((x for x in phone_numbers if x.get('type') == "primary"), None)
        if primary_number:
            customer["PrimaryPhone"] = {
                "FreeFormNumber": primary_number['number']
            }

    addresses = record.get("addresses")

    if addresses:
        if isinstance(addresses, str):
            addresses = eval(addresses)

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


def item_from_unified(record):

    mapp = {
        "name": "Name",
        "active": "Active",
        "type": "Type",
        "category": "FullyQualifiedName",
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
        item["ExpenseAccountNum"] = billItem.get("accountId")

    if record.get("isInvoiceItem", False) and record.get("invoiceItem"):
        invoiceItem = record["invoiceItem"]
        if isinstance(invoiceItem, str):
            invoiceItem = eval(invoiceItem)

        item["Description"] = invoiceItem.get("description")
        item["IncomeAccountNum"] = invoiceItem.get("accountId")
        item["UnitPrice"] = invoiceItem.get("unitPrice")

    # Hardcoding "QtyOnHand" = 0 if "type" == "Inventory"
    if item["Type"] == "Inventory":
        today = datetime.datetime.now()
        item["InvStartDate"] = today.strftime("%Y-%m-%d")
        item["TrackQtyOnHand"] = True
        item["QtyOnHand"] = 0.0

    return item


def invoice_line(items, products, tax_codes=None):

    lines = []
    if isinstance(items, str):
        items = json.loads(items)

    for item in items:
        product = products[item.get("productName")]
        product_id = product["Id"]

        item_line_detail = {
            "ItemRef": {"value": product_id},
            "Qty": item.get("quantity"),
            "UnitPrice": item.get("unitPrice"),
            # "TaxInclusiveAmt": item.get('taxAmount'),
            # "DiscountAmt" : item.get('discountAmount')  #Implemented below
        }

        if tax_codes and item.get("taxCode") is not None:
            item_line_detail.update(
                {"TaxCodeRef": {"value": tax_codes[item["taxCode"]]["Id"]}}
            )

        line_item = {
            "DetailType": "SalesItemLineDetail",
            "Amount": item.get("totalPrice"),
            "SalesItemLineDetail": item_line_detail,
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
            lines.append(
                {
                    "DetailType": "DiscountLineDetail",
                    "Amount": item.get("totalPrice"),
                    "Description": "Less discount",
                    "DiscountLineDetail": {
                        "PercentBased": True,
                        "DiscountPercent": str(
                            100 * (item.get("discountAmount") / item.get("totalPrice"))
                        ),
                    },
                }
            )

    return lines


def invoice_from_unified(record, customers, products, tax_codes):
    customer_id = customers[record.get("customerName")]["Id"]

    invoice_lines = invoice_line(record.get("lineItems"), products, tax_codes)

    invoice = {
        "Line": invoice_lines,
        "CustomerRef": {"value": customer_id},
        "TotalAmt": record.get("totalAmount"),
        "DueDate": record.get("dueDate").split("T")[0],
        "DocNumber": record.get("invoiceNumber"),
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


def credit_line(items, products, tax_codes=None):

    lines = []
    if isinstance(items, str):
        items = json.loads(items)

    for item in items:
        product = products[item.get("productName")]
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
