from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound

class CustomerSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        {"record_field": "fullName", "qbo_field": "DisplayName", "required_if_present": False}
    ]

    field_mappings = {
        "companyName": "CompanyName",
        "fullName": "DisplayName",
        "firstName": "GivenName",
        "middleName": "MiddleName",
        "lastName": "FamilyName",
        "suffix": "Suffix",
        "title": "Title",
        "taxCode": "PrimaryTaxIdentifier",
        "notes": "Notes",
        "isActive": "Active"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_email(),
            **self._map_website(),
            **self._map_currency(),
            **self._map_phone_numbers({"primary": "PrimaryPhone", "secondary": "AlternatePhone", "fax": "Fax", "mobile": "Mobile"}),
            **self._map_addresses(),
            **self._map_parent(),
            **self._map_payment_method(),
            **self._map_taxable(),
            **self._map_customer_ref_type()
        }

        self._map_fields(payload)

        return payload

    def _map_parent(self):
        found_parent = None

        if parent_id := self.record.get("parentId"):
            found_parent = next(
                (customer for customer in self.reference_data.get("Customers", [])
                if customer["Id"] == parent_id),
                None
            )

        if (parent_name := self.record.get("parentName")) and found_parent is None:
            found_parent = next(
                (customer for customer in self.reference_data.get("Customers", [])
                if customer["DisplayName"] == parent_name),
                None
            )

        if (parent_id or parent_name) and found_parent is None:
            raise RecordNotFound(f"Parent Customer could not be found in QBO with Id={parent_id} / Name={parent_name}")

        if found_parent:
            return {
                "ParentRef": {"value": found_parent["Id"], "name": found_parent["DisplayName"]},
                "Job": True
            }
        
        return {}
    
    def _map_payment_method(self):
        found_payment_method = None

        if payment_method_name := self.record.get("paymentMethod"):
            found_payment_method = next(
                (payment_method for payment_method in self.reference_data.get("PaymentMethods", [])
                if payment_method["Name"] == payment_method_name),
                None
            )

        if payment_method_name and found_payment_method is None:
            raise RecordNotFound(f"Payment Method could not be found in QBO with Name={payment_method_name}")

        if found_payment_method:
            return {"PaymentMethodRef": {"value": found_payment_method["Id"], "name": found_payment_method["Name"]}}

        return {}

    def _map_customer_ref_type(self):
        found_customer_type = None

        if cust_type_id := self.record.get("categoryId"):
            found_customer_type = next(
                (cust_type for cust_type in self.reference_data.get("CustomerTypes", [])
                if cust_type["Id"] == cust_type_id),
                None
            )

        if (cust_type_name := self.record.get("categoryName")) and found_customer_type is None:
            found_customer_type = next(
                (cust_type for cust_type in self.reference_data.get("CustomerTypes", [])
                if cust_type["Name"] == cust_type_name),
                None
            )

        if (cust_type_id or cust_type_name) and found_customer_type is None:
            raise RecordNotFound(f"Customer Type could not be found in QBO with Id={cust_type_id} / Name={cust_type_name}")

        if found_customer_type:
            return {"CustomerTypeRef": {"value": found_customer_type["Id"]}}
        
        return {}

    def _map_taxable(self):
        taxable_info = {}

        if taxable := self.record.get("taxable"):
            taxable_info["Taxable"] = taxable
            if taxable is False:
                taxable_info["TaxExemptionReasonId"] = self.record.get("taxExemptionReasonId")

        return taxable_info
