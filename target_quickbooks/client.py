import json
import requests
from datetime import datetime
from intuitlib.client import AuthClient
from singer_sdk.plugin_base import PluginBase
from target_hotglue.client import HotglueBatchSink
from typing import Dict, List, Optional

class QuickbooksSink(HotglueBatchSink):
    endpoint = "/batch"
    max_size = 30  # Max records to write in one batch

    @property
    def base_url(self) -> str:
        realm = self.config.get("realmId")

        return (
            f"https://sandbox-quickbooks.api.intuit.com/v3/company/{realm}"
            if self.config.get("is_sandbox")
            else f"https://quickbooks.api.intuit.com/v3/company/{realm}"
        )

    @property
    def authenticator(self):
        auth_credentials = {"Authorization": f"Bearer {self.access_token}"}
        return auth_credentials

    @property
    def get_url_params(self):
        params = {}
        params["minorversion"] = 40  # minorversion=40
        return params

    def __init__(self, target: PluginBase, stream_name: str, schema: Dict, key_properties: Optional[List[str]]) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        # Save config for refresh_token saving
        self.config_file = target._config_file_path

        # Instantiate Client
        self.instantiate_client()

        # Get reference data
        self.get_reference_data()

    def validate_input(self, record: dict):
        return True

    def instantiate_client(self):
        self.last_refreshed = None
        self.access_token = self.config.get("access_token")
        self.refresh_token = self.config.get("refresh_token")

        client_id = self.config.get("client_id")
        client_secret = self.config.get("client_secret")
        redirect_uri = self.config.get("redirect_uri")

        if self.config.get("is_sandbox"):
            environment = "sandbox"
        else:
            environment = "production"

        self.auth_client = AuthClient(
            client_id, client_secret, redirect_uri, environment
        )

        if not self.is_token_valid():
            self.update_access_token()

    def get_reference_data(self):
        self.accounts = self.get_entities("Account", key="AcctNum")
        self.accounts_name = self.get_entities("Account", key="Name")
        self.customers = self.get_entities("Customer", key="DisplayName")
        self.items = self.get_entities("Item", key="Name")
        self.classes = self.get_entities("Class")
        self.tax_codes = self.get_entities("TaxCode")
        self.vendors = self.get_entities("Vendor", key="DisplayName")
        self.terms = self.get_entities("Term", key="Name")
        self.customer_type = self.get_entities("CustomerType", key="Name")
        self.payment_methods = self.get_entities("PaymentMethod", key="Name")
        self.sales_terms = self.get_entities("Term")

    def update_access_token(self):
        self.auth_client.refresh(self.config.get("refresh_token"))
        self.access_token = self.auth_client.access_token
        self.refresh_token = self.auth_client.refresh_token
        self._config["refresh_token"] = self.refresh_token
        self._config["access_token"] = self.access_token
        self._config["last_update"] = round(datetime.now().timestamp())

        with open(self.config_file, "w") as outfile:
            json.dump(self._config, outfile, indent=4)

    def is_token_valid(self):
        last_update = self.config.get("last_update")
        if not last_update:
            return False
        if round(datetime.now().timestamp()) - last_update > 3300:  # 1h - 5min
            return False
        return True

    def start_batch(self, context: dict) -> None:
        if not self.is_token_valid():
            # If the token is invalid, refresh the access token
            self.update_access_token()

    def search_reference_data(self, reference_data, key, value):
        return_data = {}
        for data in reference_data:
            if key in data:
                if data[key] == value:
                    return data
        return return_data

    def get_entities(
        self, entity_type, key="Name", fallback_key="Name", check_active=True , where_filter=None
    ):
        access_token = self.access_token
        offset = 0
        max = 100
        entities = {}

        while True:
            query = f"select * from {entity_type}"
            if check_active:
                query = query + " where Active=true"

            if where_filter and check_active==False:
               query = query + f" where {where_filter}" 
                

            query = query + f" STARTPOSITION {offset} MAXRESULTS {max}"

            self.logger.info(f"Fetch {entity_type}; url={self.base_url}; query {query}; minorversion 40")
            
            r = self.request_api(
                "GET",
                endpoint="/query",
                params={"query": query, "minorversion": "40"},
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {access_token}",
                },
            )


            response = r.json()

            # Establish number of records returned.
            count = response["QueryResponse"].get("maxResults")

            # No results - exit loop.
            if not count or count == 0:
                break

            # Parse the results
            records = response["QueryResponse"][entity_type]

            if not records:
                records = []

            # Append the results
            for record in records:
                entity_key = record.get(key, record.get(fallback_key))
                # Ignore None keys
                if entity_key is None:
                    self.logger.warning(f"Failed to parse record f{json.dumps(record)}")
                    continue

                entities[entity_key] = record

            # We're done - exit loop
            if count < max:
                break

            offset += max

        self.logger.debug(f"[get_entities]: Found {len(entities)} {entity_type}.")

        return entities

    def process_batch_record(self, record: dict, index: int) -> dict:
        return {"bId": f"bid{index}", "operation": record[2], record[0]: record[1]}

    def make_batch_request(self, batch_requests, params={}):
        access_token = self.access_token

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

        if not params.get("minorversion"):
            params["minorversion"] = "4"

        r = self.request_api(
            "POST",
            headers=headers,
            params=params,
            request_data={"BatchItemRequest": batch_requests},
        )

        response = r.json()

        self.logger.info(f"DEBUG RESPONSE: {response}")

        if response.get("Fault") is not None:
            self.logger.error(response)

        return response.get("BatchItemResponse")

    def handle_batch_response(self, response):
        response_items = response or []

        posted_records = []
        failed = False

        entities = [
            "JournalEntry",
            "Customer",
            "Item",
            "Invoice",
            "CreditMemo",
            "Bill"
        ]

        for ri in response_items:
            if ri.get("Fault") is not None:
                self.logger.error(f"Failure creating entity error=[{json.dumps(ri)}]")
                failed = True
                posted_records.append({
                    "success": False,
                    "error": ri.get("Fault").get("Error")
                })
            else:
                for entity in entities:
                    if not ri.get(entity):
                        continue

                    record = ri.get(entity)

                    posted_records.append({
                        "Id": record.get("Id"),
                        "SyncToken": record.get("SyncToken"),
                        "Entity": entity,
                        "entityData": record,
                        "success": True,
                    })

        if failed:
            batch_requests = []
            # In the event of failure, we need to delete the posted records
            for i, raw_record in enumerate(posted_records):
                if not raw_record.get("entityData"):
                    continue

                raw_record["success"] = False

                record = raw_record.copy()

                entity = record["Entity"]
                entity_data = record["entityData"]

                batch_requests.append({
                    "bId": f"bid{i}",
                    "operation": "delete",
                    entity: entity_data,
                })

            # Do delete batch requests
            self.logger.info("Deleting any posted records entries...")
            response = self.make_batch_request(batch_requests)
            self.logger.debug(json.dumps(response))

        def format_record(record: dict):
            record.pop("Entity", None)
            return record

        posted_records = list(map(format_record, posted_records))

        return {"state_updates": posted_records}

