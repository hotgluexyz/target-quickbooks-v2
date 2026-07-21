from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound, InvalidInputError


class ClassSchemaMapper(BaseMapper):
    """
    Mapper for QuickBooks Class entity.
    Note: FullyQualifiedName is the unique key in QuickBooks, not Name alone.
    Name can collide across different parent classes.
    """
    existing_record_pk_mappings = [
        {"record_field": "id", "qbo_field": "Id", "required_if_present": True},
        # For name-based upsert, we need to compute FullyQualifiedName
    ]

    field_mappings = {
        "externalId": "externalId",
        "name": "Name",
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            **self._map_internal_id_by_fqn(),
            **self._map_parent(),
        }

        self._map_is_active(payload)
        self._map_fields(payload)

        return payload

    def _map_internal_id_by_fqn(self):
        """
        Map internal ID using FullyQualifiedName for uniqueness.
        Unlike other entities, Class uses FQN as the unique key.
        """
        # First try by ID
        if record_id := self.record.get("id"):
            # Normalize to string for comparison
            record_id_str = str(record_id)
            found_class = next(
                (cls for cls in self.reference_data.get(self.sink_name, [])
                 if str(cls.get("Id")) == record_id_str),
                None
            )
            if found_class:
                return {
                    "Id": found_class["Id"],
                    "SyncToken": found_class["SyncToken"],
                    "sparse": True
                }

        # Try by FullyQualifiedName (computed from name + parent)
        if class_name := self.record.get("name"):
            # Compute expected FullyQualifiedName
            expected_fqn = self._compute_fqn(class_name)
            
            # Search for matching FQN in the list
            found_class = next(
                (cls for cls in self.reference_data.get(self.sink_name, [])
                 if cls.get("FullyQualifiedName") == expected_fqn),
                None
            )
            
            if found_class:
                return {
                    "Id": found_class["Id"],
                    "SyncToken": found_class["SyncToken"],
                    "sparse": True
                }

        return {}

    def _compute_fqn(self, class_name: str) -> str:
        """
        Compute FullyQualifiedName for a class.
        For top-level: FQN = Name
        For subclass: FQN = ParentFQN:Name
        """
        parent_id = self.record.get("parentId")
        parent_name = self.record.get("parentName")
        
        if not (parent_id or parent_name):
            # Top-level class
            return class_name
        
        # Find parent to get its FQN
        parent = self._find_parent()
        if parent:
            parent_fqn = parent.get("FullyQualifiedName", parent.get("Name"))
            return f"{parent_fqn}:{class_name}"
        
        # If parent not found yet (being created in same batch), return just name
        return class_name

    def _map_parent(self):
        """Map parent reference for subclasses."""
        parent_id = self.record.get("parentId")
        parent_name = self.record.get("parentName")
        
        if not (parent_id or parent_name):
            return {}
        
        found_parent = self._find_parent()

        if (parent_id or parent_name) and found_parent is None:
            raise RecordNotFound(
                f"Parent Class could not be found in QBO with Id={parent_id} / Name={parent_name}"
            )

        if found_parent:
            return {
                "ParentRef": {"value": found_parent["Id"], "name": found_parent["Name"]},
                "SubClass": True
            }
        
        return {}

    def _find_parent(self):
        """Find parent class by ID or Name.

        Name-based resolution prefers the unique FullyQualifiedName. Since the
        QBO Name field is only the leaf name and can collide across different
        parents, we only fall back to Name when it resolves unambiguously.
        """
        parent_id = self.record.get("parentId")
        parent_name = self.record.get("parentName")

        classes = self.reference_data.get(self.sink_name, [])

        found_parent = None

        # Try by ID first (normalize to string)
        if parent_id:
            parent_id_str = str(parent_id)
            found_parent = next(
                (cls for cls in classes
                 if str(cls.get("Id")) == parent_id_str),
                None
            )

        # Fall back to name. FullyQualifiedName is the unique key in QBO, so
        # match on it first, then only on the (non-unique) Name field.
        if not found_parent and parent_name:
            found_parent = next(
                (cls for cls in classes
                 if cls.get("FullyQualifiedName") == parent_name),
                None
            )

        if not found_parent and parent_name:
            name_matches = [cls for cls in classes if cls.get("Name") == parent_name]

            if len(name_matches) > 1:
                fqns = ", ".join(
                    sorted(cls.get("FullyQualifiedName", cls.get("Name")) for cls in name_matches)
                )
                raise InvalidInputError(
                    f"parentName={parent_name} is ambiguous in QBO; it matches "
                    f"multiple classes ({fqns}). Provide the fully qualified parentName "
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
                    "Invalid value isActive=False when creating a new Class. "
                    "Only existing Classes can be de-activated."
                )
            payload["Active"] = is_active
