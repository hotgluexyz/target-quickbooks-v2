import json
from copy import deepcopy
from typing import Dict, List, Optional

from singer_sdk.plugin_base import PluginBase

from target_hotglue.client import HotglueBatchSink
from target_hotglue.client import HotglueBatchSink
from target_quickbooks.quickbooks_client import QuickbooksClient


class QuickbooksBatchSink(HotglueBatchSink):
    max_size = 30  # Max records to write in one batch

    def __init__(self, target: PluginBase, stream_name: str, schema: Dict, key_properties: Optional[List[str]]) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self.quickbooks_client: QuickbooksClient = target.quickbooks_client
        self.reference_data = self._target.reference_data

    def validate_input(self, record: dict):
        return True    

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

    def process_batch(self, context: dict) -> None:
        # If the latest state is not set, initialize it
        if not self.latest_state:
            self.init_state()
        
        # Extract the raw records from the context
        raw_records = context.get("records", [])

        reference_data = self.get_batch_reference_data(raw_records)

        records = []
        for raw_record in enumerate(raw_records):
            try:
                # performs record mapping from unified to QBO
                record = self.process_batch_record(raw_record[1], raw_record[0], reference_data)
                records.append(record)
            except Exception as e:
                state = {"success": False, "error": str(e)}
                if id := raw_record[1].get("id"):
                    state["id"] = str(id)
                if external_id := raw_record[1].get("externalId"):
                    state["externalId"] = external_id
                self.update_state(state)

        response = self.make_batch_request(records)
        # Handle the batch response 
        result = self.handle_batch_response(response, records)
        state_updates = result.get("state_updates", [])

        # Update the latest state for each state update in the response
        for state_update in state_updates:
            self.update_state(state_update)

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
            "Bill",
            "BillPayment",
            "Customer",
            "Invoice",
            "Item",
            "JournalEntry",
            "Payment",
            "PurchaseOrder",
            "Vendor",
            "VendorCredit"
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
