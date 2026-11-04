import json
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.plugin_base import PluginBase
from hotglue_singer_sdk.target_sdk.client import HotglueBatchSink

from target_quickbooks.mappers.base_mapper import ParentNotFound, _MAX_NESTING_LEVELS
from target_quickbooks.quickbooks_client import QuickbooksClient


QBO_BATCH_SIZE = 30


class QuickbooksBatchSink(HotglueBatchSink):
    max_size = QBO_BATCH_SIZE
    # Hierarchical streams set this so parent+child in one job share one process_batch
    buffer_until_stream_end = False

    @property
    def is_full(self) -> bool:
        if self.buffer_until_stream_end:
            return False
        return self.current_size >= self.max_size

    def __init__(self, target: PluginBase, stream_name: str, schema: Dict, key_properties: Optional[List[str]]) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self.quickbooks_client: QuickbooksClient = target.quickbooks_client
        self.reference_data = self._target.reference_data

    def validate_input(self, record: dict):
        return True

    def _get_error_classification_metadata(self, error: Exception) -> dict:
        if isinstance(error, InvalidCredentialsError):
            return {"hg_error_class": InvalidCredentialsError.__name__}
        if isinstance(error, InvalidPayloadError):
            return {"hg_error_class": InvalidPayloadError.__name__}
        return {}

    def get_batch_reference_data(self, records: List) -> dict:
        """Get the reference data for a batch

        Args:
            records: List of records to be processed by the batch

        Returns:
            A dict containing batch specific reference data.
        """
        return self._target.reference_data

    def process_batch_record(self, record: dict, index: int, reference_data: dict) -> dict:
        return {"bId": f"bid{index}", "operation": record[2], record[0]: record[1]}

    @staticmethod
    def _record_has_parent(record: dict) -> bool:
        return bool(record.get("parentId") or record.get("parentName"))

    def _fail_mapping_error(self, raw_record: dict, e: Exception) -> None:
        state = {"success": False, "error": str(e)}
        state.update(self._get_error_classification_metadata(e))
        if id := raw_record.get("id"):
            state["id"] = str(id)
        if external_id := raw_record.get("externalId"):
            state["externalId"] = external_id
        # not adding record here, because it failed during mapping
        self.update_state(state)


    def _merge_created_entities_into_reference_data(
        self, response: Optional[List[dict]], reference_data: dict
    ) -> None:
        if not response:
            return

        by_id = {
            str(entity["Id"]): entity
            for entity in reference_data.get(self.name, [])
            if entity.get("Id") is not None
        }
        for item in response:
            if item.get("Fault"):
                continue
            entity = item.get(self.record_type)
            if entity and entity.get("Id") is not None:
                by_id[str(entity["Id"])] = entity

        merged = list(by_id.values())
        reference_data[self.name] = merged
        self._target.reference_data[self.name] = merged

    def _submit_batch_wave(self, mapped_records: List[dict], reference_data: dict) -> None:
        for offset in range(0, len(mapped_records), QBO_BATCH_SIZE):
            chunk = mapped_records[offset : offset + QBO_BATCH_SIZE]
            response = self.make_batch_request(chunk)
            # Handle the batch response 
            result = self.handle_batch_response(response, chunk)
            for i, state_update in enumerate(result.get("state_updates", [])):
                self.update_state(state_update, record=chunk[i])
            # we need to append the newly created parent entities so they can be used to resolve the children in the next wave
            self._merge_created_entities_into_reference_data(response, reference_data)



    def _process_parent_waves(
        self, pending: List[Tuple[int, dict]], reference_data: dict
    ) -> None:
        self.logger.info(f"Processing batch waves for {self.record_type} were parent child nesting can be present on the same payload")
        for wave_index in range(_MAX_NESTING_LEVELS+1):
            self.logger.info(f"Processing wave {wave_index+1} for {self.record_type}.")
            if not pending:
                return

            wave_mapped = []
            deferred = []

            for index, raw_record in pending:
                try:
                    wave_mapped.append(
                        self.process_batch_record(raw_record, index, reference_data)
                    )
                except ParentNotFound as error:
                    #same logic as process_batch; but we catch the failed by missing parent and retry on the next wave
                    deferred.append((index, raw_record, error))
                except Exception as error:
                    self._fail_mapping_error(raw_record, error)

            if wave_mapped:
                self._submit_batch_wave(wave_mapped, reference_data)

            if not deferred:
                self.logger.info(f"No missing parent on this wave {self.record_type}. Stopping batch waves processing.")
                return

            if not wave_mapped:
                # we made zero progress on the last wave; fail the deferred records and stop processing
                self.logger.info(f"No record processed on the last wave {self.record_type}. Stopping batch waves processing.")
                for _, raw_record, error in deferred:
                    self._fail_mapping_error(raw_record, error)
                return

            #drop the error from the deferred records
            pending = [(index, raw_record) for index, raw_record, _ in deferred]


    def process_batch(self, context: dict) -> None:
        # If the latest state is not set, initialize it
        if not self.latest_state:
            self.init_state()
        
        # Extract the raw records from the context
        raw_records = context.get("records", [])

        reference_data = self.get_batch_reference_data(raw_records)

        if self.buffer_until_stream_end and any(self._record_has_parent(record) for record in raw_records):
            self._process_parent_waves(list(enumerate(raw_records)), reference_data)
            return

        records = []
        for raw_record in enumerate(raw_records):
            try:
                # performs record mapping from unified to QBO
                record = self.process_batch_record(raw_record[1], raw_record[0], reference_data)
                records.append(record)
            except Exception as e:
                self._fail_mapping_error(raw_record[1], e)

        self._submit_batch_wave(records, reference_data)

    def make_batch_request(self, records: List[Dict]):
        request_records = []
        for record in records:
            rec = deepcopy(record)
            rec[self.record_type].pop("externalId", None)
            request_records.append(rec)

        return self.quickbooks_client.make_batch_request(request_records)

    def handle_batch_response(self, response, records):
        response_items = response or []
        state_updates = []
        entities = [
            "Account",
            "Bill",
            "BillPayment",
            "Class",
            "Customer",
            "Invoice",
            "Item",
            "JournalEntry",
            "Payment",
            "PurchaseOrder",
            "Vendor",
            "VendorCredit",
            "Term",
        ]

        for ri in response_items:
            record_payload = next((record for record in records if record.get("bId") == ri.get("bId")), {})

            if ri.get("Fault") is not None:
                self.logger.error(f"Failure creating entity error=[{json.dumps(ri)}]")
                state_updates.append({
                    "success": False,
                    "externalId": record_payload.get(self.record_type, {}).get("externalId"),
                    "error": ri.get("Fault").get("Error")
                })
            else:
                for entity in entities:
                    if not ri.get(entity):
                        continue

                    resulting_record = ri.get(entity)

                    state = {
                        "id": resulting_record.get("Id"),
                        "externalId": record_payload.get(entity, {}).get("externalId"),
                        "success": True,
                    }

                    if record_payload.get("operation") == "update":
                        state["is_updated"] = True
                    
                    state_updates.append(state)

        return {"state_updates": state_updates}

    def error_to_string(self, error: Any):
        if isinstance(error, list):
            return ". ".join([self.error_to_string(item) for item in error])
        if isinstance(error, dict) and "Detail" in error:
            return error.get("Detail")
        else:
            return str(error)
