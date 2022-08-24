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
        "active": "Active",
    }

    customer = dict(
        (mapp[key], value) for (key, value) in record.items() if key in mapp.keys()
    )

    customer["PrimaryEmailAddr"] = {"Address": record.get("emailAddress", "")}

    addresses = record.get("addresses")

    if addresses:
        if isinstance(addresses, str):
            addresses = eval(addresses)

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

    if record.get("billItem"):
        billItem = record["billItem"]
        if isinstance(billItem, str):
            billItem = eval(billItem)

        item["UnitPrice"] = billItem.get("unitPrice")

        item["Description"] = billItem.get("description")

        item["IncomeAccountRef"] = {"value": billItem.get("accountId")}

    if record.get("invoiceItem"):
        invoiceItem = record["invoiceItem"]
        if isinstance(invoiceItem, str):
            invoiceItem = eval(invoiceItem)

        item["PurchaseDesc"] = invoiceItem.get("description")

        item["ExpenseAccountRef"] = {"value": invoiceItem.get("accountId")}

        item["PurchaseCost"] = invoiceItem.get("unitPrice")

    # Hardcoding "QtyOnHand" = 0 if "type" == "Inventory"
    if item["Type"] == "Inventory":
        item["QtyOnHand"] = 0.0

    return item


def invoice_line(items, products, tax_codes):

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
            }

        if tax_codes and item.get('taxCode') is not None:
            item_line_detail.update({"TaxCodeRef": {
                    "value": tax_codes[item['taxCode']]['Id']
                }})

        line_item = {
            "DetailType": "SalesItemLineDetail",
            "Amount": item.get("totalPrice"),
            "SalesItemLineDetail": item_line_detail
        }

        if product["TrackQtyOnHand"]:
            if product["QtyOnHand"] < 1:
                logging.info(
                    f"No quantity available for Product: {item.get('productName')}"
                )
                line_item = None

        if line_item:
            lines.append(line_item)

    return lines


def invoice_from_unified(record, customers, products, tax_codes):
    customer_id = customers[record.get("customerName")]["Id"]

    invoice_lines = invoice_line(record.get("lineItems"), products, tax_codes)

    invoice = {
        "Line": invoice_lines,
        "CustomerRef": {"value": customer_id},
        "TotalAmt": record.get("totalAmount"),
        "DueDate": record.get("dueDate").split("T")[0],
        "DocNumber": record.get("invoiceNumber")
    }

    if not invoice_lines:
        logging.warn(
            f"No Invoice Lines for Invoice id: {record['id']} \n Skipping Invoice ..."
        )
        return []

    return invoice
