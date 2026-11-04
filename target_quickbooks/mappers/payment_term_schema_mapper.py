from typing import Dict

from target_quickbooks.mappers.base_mapper import BaseMapper, InvalidInputError, RecordNotFound


class PaymentTermSchemaMapper(BaseMapper):
    existing_record_pk_mappings = []

    field_mappings = {
        "externalId": "externalId",
        "name": "Name",
        "discountPercent": "DiscountPercent",
    }

    _INT_FIELD_MAPPINGS = {
        "discountDays": "DiscountDays",
        "dayOfMonthDue": "DayOfMonthDue",
        "dueNextMonthDays": "DueNextMonthDays",
        "dueDays": "DueDays",
        "discountDayOfMonth": "DiscountDayOfMonth",
    }

    def to_quickbooks(self) -> Dict:
        if not self.record.get("name"):
            raise InvalidInputError("Payment term name is required")

        payload = {
            **self._map_existing_by_id_or_name(),
        }

        self._map_active(payload)
        self._map_fields(payload)
        self._map_int_fields(payload)

        return payload

    def _map_existing_by_id_or_name(self) -> Dict:
        terms = self.reference_data.get(self.sink_name, [])

        if record_id := self.record.get("id"):
            record_id_str = str(record_id)
            found_term = next(
                (term for term in terms if str(term.get("Id")) == record_id_str),
                None,
            )
            if not found_term:
                raise RecordNotFound(
                    f"Payment term {record_id} not found. Skipping..."
                )
            return {
                "Id": found_term["Id"],
                "SyncToken": found_term["SyncToken"],
                "sparse": True,
            }

        if term_name := self.record.get("name"):
            found_term = next(
                (term for term in terms if term.get("Name") == term_name),
                None,
            )
            if found_term:
                return {
                    "Id": found_term["Id"],
                    "SyncToken": found_term["SyncToken"],
                    "sparse": True,
                }

        return {}

    def _map_active(self, payload: Dict) -> None:
        active = self.record.get("active")
        if active is None:
            active = self.record.get("isActive")
        if active is not None:
            payload["Active"] = active

    def _map_int_fields(self, payload: Dict) -> None:
        for record_key, payload_key in self._INT_FIELD_MAPPINGS.items():
            if record_key in self.record and self.record[record_key] is not None:
                payload[payload_key] = int(self.record[record_key])
