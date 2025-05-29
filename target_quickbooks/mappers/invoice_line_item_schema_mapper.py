from typing import Dict
from target_quickbooks.mappers.base_mapper import BaseMapper, RecordNotFound

class InvoiceLineItemSchemaMapper(BaseMapper):
    existing_record_pk_mappings = []

    field_mappings = {
        "": ""
    }

    def to_quickbooks(self) -> Dict:
        payload = {
            
        }

        self._map_fields(payload)

        return payload
