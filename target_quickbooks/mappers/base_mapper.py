import datetime
from typing import Dict, List, Optional

class InvalidInputError(Exception):
    pass

class RecordNotFound(InvalidInputError):
    pass

class BaseMapper:
    """A base class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""
    existing_record_pk_mappings = []

    def __init__(
            self,
            record,
            sink_name,
            reference_data
    ) -> None:
        self.record = record
        self.sink_name = sink_name
        self.reference_data = reference_data
        self.existing_record = self._find_existing_record(self.reference_data.get(self.sink_name, []))

    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal.
        """        

        for existing_record_pk_mapping in self.existing_record_pk_mappings:
            if record_id := self.record.get(existing_record_pk_mapping["record_field"]):
                found_record = next(
                    (qbo_record for qbo_record in reference_list
                    if str(qbo_record[existing_record_pk_mapping["qbo_field"]]) == str(record_id)),
                    None
                )
                if existing_record_pk_mapping["required_if_present"] and found_record is None:
                    raise RecordNotFound(f"Record {existing_record_pk_mapping['record_field']}={record_id} not found in QBO. Skipping it")
                
                if found_record:
                    return found_record
        
        return None

    def _map_internal_id(self):
        if self.existing_record:
            return {
                "Id": self.existing_record["Id"],
                "SyncToken": self.existing_record["SyncToken"],
                "sparse": True
            }

        return {}

    def _map_fields(self, payload, custom_field_mappings={}):
        field_mappings = self.field_mappings

        if custom_field_mappings:
            field_mappings = custom_field_mappings

        for record_key, payload_key in field_mappings.items():
            if record_key in self.record and self.record.get(record_key) != None:
                if isinstance(payload_key, list):
                    for key in payload_key:
                        payload[key] = self.record.get(record_key)
                else:
                    record_value = self.record.get(record_key)
                    if isinstance(record_value, datetime.datetime):
                        payload[payload_key] = record_value.isoformat()
                    else:
                        payload[payload_key] = record_value

    def _map_email(self):
        if email := self.record.get("email"):
            return {"PrimaryEmailAddr": {"Address": email}}
        return {}
    
    def _map_website(self):
        if website := self.record.get("website"):
            return {"WebAddr": {"URI": website}}
        return {}
    
    def _map_currency(self):
        found_currency = None
        currency_info = {}

        if currency_id := self.record.get("currencyId"):
            found_currency = next(
                (currency for currency in self.reference_data["Currencies"]
                if currency["Id"] == currency_id),
                None
            )

        if (currency_code := self.record.get("currency")) and not found_currency:
            found_currency = next(
                (currency for currency in self.reference_data["Currencies"]
                if currency["Code"] == currency_code),
                None
            )

        if (currency_name := self.record.get("currencyName")) and not found_currency:
            found_currency = next(
                (currency for currency in self.reference_data["Currencies"]
                if currency["Name"] == currency_name),
                None
            )

        if found_currency:
            currency_info = {
                "CurrencyRef": {
                    "value": found_currency["Code"],
                    "name": found_currency["Name"]
                }
            }

        return currency_info
    
    def _map_phone_numbers(self, phone_types_map):
        """Extracts phone numbers in QBO format."""
        phones = {}

        if not self.record.get("phoneNumbers"):
            return {}

        for phone_type_from, phone_type_to in phone_types_map.items():
            found_phone_number = next(
                (phone for phone in self.record.get("phoneNumbers")
                if phone["type"] == phone_type_from),
                None
            )

            if not found_phone_number:
                continue

            phones[phone_type_to] = {"FreeFormNumber": found_phone_number["phoneNumber"]}

        return phones
    
    def _map_addresses(self, addresses_types_map):
        """Extracts phone numbers in QBO format."""
        addresses = {}

        if not self.record.get("addresses"):
            return {}

        for address_type_from, address_type_to in addresses_types_map.items():
            found_address = next(
                (address for address in self.record.get("addresses")
                if address["addressType"] == address_type_from),
                None
            )

            if not found_address:
                continue

            addresses[address_type_to] = {
                "Line1": found_address.get("line1"),
                "Line2": found_address.get("line2"),
                "Line3": found_address.get("line3"),
                "City": found_address.get("city"),
                "CountrySubDivisionCode": found_address.get("state"),
                "PostalCode": found_address.get("postalCode"),
                "Country": found_address.get("country")
            }

        return addresses
    
    def _map_customer(self):
        found_customer = None

        if customer_id := self.record.get("customerId"):
            found_customer = next(
                (customer for customer in self.reference_data.get("Customers", [])
                if customer["Id"] == customer_id),
                None
            )

        if (customer_name := self.record.get("customerName")) and found_customer is None:
            found_customer = next(
                (customer for customer in self.reference_data.get("Customers", [])
                if customer["DisplayName"] == customer_name),
                None
            )

        if (customer_id or customer_name) and found_customer is None:
            raise RecordNotFound(f"Customer could not be found in QBO with Id={customer_id} / Name={customer_name}")

        if found_customer:
            return {
                "CustomerRef": {"value": found_customer["Id"], "name": found_customer["DisplayName"]}
            }
        
        return {}
    
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
            return {
                "AccountRef": {"value": found_account["Id"], "name": found_account["Name"]}
            }
        
        return {}
    
    def _map_vendor(self):
        found_vendor = None

        if vendor_id := self.record.get("vendorId"):
            found_vendor = next(
                (vendor for vendor in self.reference_data.get("Vendors", [])
                if vendor["Id"] == vendor_id),
                None
            )

        if (vendor_name := self.record.get("vendorName")) and found_vendor is None:
            found_vendor = next(
                (vendor for vendor in self.reference_data.get("Vendors", [])
                if vendor["DisplayName"] == vendor_name),
                None
            )

        if (vendor_id or vendor_name) and found_vendor is None:
            raise RecordNotFound(f"Vendor could not be found in QBO with Id={vendor_id} / Name={vendor_name}")

        if found_vendor:
            return {
                "VendorRef": {"value": found_vendor["Id"], "name": found_vendor["DisplayName"]}
            }
        
        return {}
    
    def _map_class(self):
        class_info = {}
        found_class = None

        if class_id := self.record.get("classId"):
            found_class = next(
                (_class for _class in self.reference_data["Classes"]
                if _class["Id"] == class_id),
                None
            )

        if (class_name := self.record.get("className")) and not found_class:
            found_class = next(
                (_class for _class in self.reference_data["Classes"]
                if _class["Name"] == class_name),
                None
            )

        if (class_id or class_name) and found_class is None:
            raise RecordNotFound(f"A Class with Id={class_id} / Name={class_name} could not be found in QBO")

        if found_class:
            class_info["ClassRef"] = {
                "value": found_class["Id"],
                "name": found_class["Name"]
            }

        return class_info
    
    def _map_department(self):
        department_info = {}
        found_department = None

        if department_id := self.record.get("departmentId"):
            found_department = next(
                (department for department in self.reference_data["Departments"]
                if department["Id"] == department_id),
                None
            )

        if (department_name := self.record.get("departmentName")) and not found_department:
            found_department = next(
                (department for department in self.reference_data["Departments"]
                if department["Name"] == department_name),
                None
            )

        if (department_id or department_name) and found_department is None:
            raise RecordNotFound(f"A Department with Id={department_id} / Name={department_name} could not be found in QBO")

        if found_department:
            department_info["DepartmentRef"] = {
                "value": found_department["Id"],
                "name": found_department["Name"]
            }

        return department_info
    
    def _map_transaction_tax_code(self):
        """Maps the tax code for a transaction."""
        tax_code_info = {}
        found_tax_code = None

        if tax_code := self.record.get("taxCode"):
            found_tax_code = next(
                (tax for tax in self.reference_data["TaxCodes"]
                if tax["Name"] == tax_code),
                None
            )

        if tax_code and found_tax_code is None:
            raise RecordNotFound(f"A TaxCode with Name={tax_code} could not be found in QBO")

        if found_tax_code:
            tax_code_info["TxnTaxDetail"] = {
                "TxnTaxCodeRef": {
                    "value": found_tax_code["Id"],
                    "name": found_tax_code["Name"]
                }
            }

        return tax_code_info
    
    def _map_transaction_line_tax_code(self):
        tax_code_info = {}

        if tax_code := self.record.get("taxCode"):
            if tax_code not in ["TAX", "NON"]:
                raise InvalidInputError(f"Invalid value {tax_code} for line taxCode, it should be either 'TAX' or 'NON'")

            tax_code_info["TaxCodeRef"] = {
                "value": tax_code,
                "name": tax_code
            }

        return tax_code_info
