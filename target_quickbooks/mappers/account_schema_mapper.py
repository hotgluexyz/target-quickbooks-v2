from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound, InvalidInputError


class AccountSchemaMapper(BaseMapper):
    """
    Mapper for QuickBooks Account entity.
    Note: FullyQualifiedName is the unique key in QuickBooks, not Name alone.
    Name can collide across different parent accounts.
    """
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
    ]

    field_mappings = {
        "externalId": "externalId",
        "name": "Name",
        "description": "Description",
        "accountNumber": "AcctNum",
        "category": "AccountType",
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_existing_by_id_or_fqn(),
            **self._map_parent(),
            **self._map_currency(),
        }

        self._validate_create_required(payload)
        self._map_is_active(payload)
        self._map_fields(payload)

        return payload

    def _validate_create_required(self, payload):
        if payload.get("Id") is None and self.record.get("category") is None:
            raise InvalidInputError(
                "category is required when creating a new Account."
            )

    def _map_existing_by_id_or_fqn(self):
        """Match an existing Account by Id, else by computed FullyQualifiedName.

        Id is the unique key in QBO. Name alone is not unique across parents, so
        name-based upsert uses FullyQualifiedName (ParentFQN:Name, or Name at
        top level).
        """
        if record_id := self.record.get("id"):
            record_id_str = str(record_id)
            found_account = next(
                (acct for acct in self.reference_data.get(self.sink_name, [])
                 if str(acct.get("Id")) == record_id_str),
                None
            )
            if found_account:
                return {
                    "Id": found_account["Id"],
                    "SyncToken": found_account["SyncToken"],
                    "sparse": True
                }

        if account_name := self.record.get("name"):
            expected_fqn = self._compute_fqn(account_name)

            found_account = next(
                (acct for acct in self.reference_data.get(self.sink_name, [])
                 if acct.get("FullyQualifiedName") == expected_fqn),
                None
            )

            if found_account:
                return {
                    "Id": found_account["Id"],
                    "SyncToken": found_account["SyncToken"],
                    "sparse": True
                }

        return {}

    def _compute_fqn(self, account_name: str) -> str:
        """
        Compute FullyQualifiedName for an account.
        For top-level: FQN = Name
        For sub-account: FQN = ParentFQN:Name
        """
        parent_id = self.record.get("parentId")
        parent_name = self.record.get("parentName")

        if not (parent_id or parent_name):
            return account_name

        parent = self._find_parent()
        if parent:
            parent_fqn = parent.get("FullyQualifiedName", parent.get("Name"))
            return f"{parent_fqn}:{account_name}"

        # If parent not found yet (being created in same batch), return just name
        return account_name

    def _map_parent(self):
        """Map parent reference for sub-accounts."""
        parent_id = self.record.get("parentId")
        parent_name = self.record.get("parentName")

        if not (parent_id or parent_name):
            return {}

        found_parent = self._find_parent()

        if (parent_id or parent_name) and found_parent is None:
            raise RecordNotFound(
                f"Parent Account could not be found in QBO with Id={parent_id} / Name={parent_name}"
            )

        if found_parent:
            return {
                "ParentRef": {"value": found_parent["Id"], "name": found_parent["Name"]},
                "SubAccount": True
            }

        return {}

    def _find_parent(self):
        """Find parent account by ID or Name.

        Name-based resolution prefers the unique FullyQualifiedName. Since the
        QBO Name field is only the leaf name and can collide across different
        parents, we only fall back to Name when it resolves unambiguously.
        """
        parent_id = self.record.get("parentId")
        parent_name = self.record.get("parentName")

        accounts = self.reference_data.get(self.sink_name, [])

        found_parent = None

        if parent_id:
            parent_id_str = str(parent_id)
            found_parent = next(
                (acct for acct in accounts
                 if str(acct.get("Id")) == parent_id_str),
                None
            )

        if not found_parent and parent_name:
            found_parent = next(
                (acct for acct in accounts
                 if acct.get("FullyQualifiedName") == parent_name),
                None
            )

        if not found_parent and parent_name:
            name_matches = [acct for acct in accounts if acct.get("Name") == parent_name]

            if len(name_matches) > 1:
                fqns = ", ".join(
                    sorted(acct.get("FullyQualifiedName", acct.get("Name")) for acct in name_matches)
                )
                raise InvalidInputError(
                    f"parentName={parent_name} is ambiguous in QBO; it matches "
                    f"multiple accounts ({fqns}). Provide the fully qualified parentName "
                    f"(e.g. one of: {fqns}) or a parentId to disambiguate."
                )

            if name_matches:
                found_parent = name_matches[0]

        return found_parent

    def _map_is_active(self, payload):
        """Map isActive field with validation."""
        is_active = self.record.get("isActive")
        if is_active is not None:
            if is_active is False and payload.get("Id") is None:
                raise InvalidInputError(
                    "Invalid value isActive=False when creating a new Account. "
                    "Only existing Accounts can be de-activated."
                )
            payload["Active"] = is_active
