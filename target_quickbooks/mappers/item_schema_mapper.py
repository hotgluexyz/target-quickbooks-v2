import datetime
from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound, InvalidInputError

class ItemSchemaMapper(BaseMapper):
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        {"record_field": "name", "qbo_field": "Name", "required_if_present": False}
    ]

    field_mappings = {
        "externalId": "externalId",
        "name": "Name",
        "type": "Type",
        "quantityOnHand": "QtyOnHand"
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id(),
            **self._map_class(),
            **self._map_item_vendor(),
            **self._map_accounts()
        }

        if self.record.get("type") == "Inventory":
            payload["TrackQtyOnHand"] = True
            payload["InvStartDate"] = datetime.datetime.now().isoformat()

        self._map_is_active(payload)
        self._map_fields(payload)

        return payload

    def _map_accounts(self):
        accounts_info = {}

        account_types = {
            "asset": "AssetAccountRef",
            "expense": "ExpenseAccountRef",
            "income": "IncomeAccountRef"
        }

        for item_account in self.record.get("accounts", []):
            found_account = None

            account_type = item_account.get("accountType")
            if account_type not in account_types.keys():
                raise InvalidInputError(f"Invalid accountType={account_type}")

            if accound_id := item_account.get("id"):
                found_account = next(
                    (account for account in self.reference_data.get("Accounts", [])
                        if account["Id"] == accound_id
                    ),
                    None)
            
            if not found_account and (account_number := item_account.get("accountNumber")):
                found_account = next(
                (account for account in self.reference_data.get("Accounts", [])
                    if account.get("AcctNum") == account_number
                ),
                None)

            if not found_account and (account_name := item_account.get("name")):
                found_account = next(
                (account for account in self.reference_data.get("Accounts", [])
                    if account["Name"] == account_name
                ),
                None)
            
            if found_account:
                qbo_account_type_key = account_types[account_type]
                accounts_info[qbo_account_type_key] = {
                    "value": found_account["Id"],
                    "name": found_account["Name"]
                }

        return accounts_info

    def _map_item_vendor(self):
        item_vendor_info = {}

        for item_vendor in self.record.get("itemVendors", []):
            found_vendor = next(
                (vendor for vendor in self.reference_data.get("Vendors", [])
                    if vendor["Id"] == item_vendor.get("vendorId")
                ),
                None)
            
            if not found_vendor:
                found_vendor = next(
                    (vendor for vendor in self.reference_data.get("Vendors", [])
                        if vendor["DisplayName"] == item_vendor.get("vendorName")
                    ),
                    None)
            
            if found_vendor:
                item_vendor_info["PrefVendorRef"] = {
                    "value": found_vendor["Id"],
                    "name": found_vendor["DisplayName"]
                }
                break
                

        return item_vendor_info

    def _map_is_active(self, payload):
        is_active = self.record.get("isActive")
        if is_active is not None:
            if is_active is False and payload.get("Id") is None:
                raise InvalidInputError(f"Invalid value isActive=False when creating a new record. It can only be used to delete an existing Item")
                
            payload["Active"] = is_active
