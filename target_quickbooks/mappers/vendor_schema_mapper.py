from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound, InvalidInputError

class VendorSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        {"record_field": "vendorName", "qbo_field": "DisplayName", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "vendorName": "DisplayName",
        "firstName": "GivenName",
        "middleName": "MiddleName",
        "lastName": "FamilyName",
        "suffix": "Suffix",
        "title": "Title",
        "checkName": "PrintOnCheckName"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_email(),
            **self._map_website(),
            **self._map_currency(),
            **self._map_phone_numbers({"primary": "PrimaryPhone", "secondary": "AlternatePhone", "fax": "Fax", "mobile": "Mobile"}),
            **self._map_addresses({"billing": "BillAddr"})
        }

        self._map_is_active(payload)
        self._map_fields(payload)

        return payload
    
    def _map_is_active(self, payload):
        is_active = self.record.get("isActive")
        if is_active is not None:
            if is_active is False and payload.get("Id") is None:
                raise InvalidInputError(f"Invalid value isActive=False when creating a new record. It can only be used to delete an existing Vendor")
                
            payload["Active"] = is_active
