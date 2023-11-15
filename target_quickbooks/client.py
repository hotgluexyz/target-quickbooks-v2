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
        self.currency = self.get_entities("Currency")
        self.vendors = self.get_entities("Vendor", key="DisplayName")
        self.terms = self.get_entities("Term", key="Name")
        self.customer_type = self.get_entities("CustomerType", key="Name")
        self.payment_methods = self.get_entities("PaymentMethod", key="Name")
        self.sales_terms = self.get_entities("Term")
        self.categories = self.get_entities("Item", where_filter="Type='Category'")

    def update_access_token(self):
        self.auth_client.refresh(self.config.get("refresh_token"))
        self.access_token = self.auth_client.access_token
        self.refresh_token = self.auth_client.refresh_token
        self._config["refresh_token"] = self.refresh_token
        self.logger.info("Updated refresh token: {}".format(self.refresh_token))
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
                if isinstance(data,dict):
                    if data[key] == value:
                        return data
                elif isinstance(data,str):
                    if data==value:
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
            try:
                records = response["QueryResponse"][entity_type]
            except KeyError:
                records = response["QueryResponse"][f"Company{entity_type}"]

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

    def process_batch(self, context: dict) -> None:
        # If the latest state is not set, initialize it
        if not self.latest_state:
            self.init_state()
        
        # Extract the raw records from the context
        raw_records = context["records"]

        records = list(map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(raw_records)))

        if self.stream_name == "Customers":
            # Build the URL to send the requests to
            url = f"{self.base_url}/customer"
            # Extract the "Customer" data from each record
            list_data = [data["Customer"] for data in records]
            # Send a separate request for each "Customer" data using map
            response = map(lambda data: self.make_request(url,data), list_data)
            # Handle the response for each request using the handle_response function
            result = map(lambda response: self.handle_response(response), list(response))
            # In case the response is successful and the record was marked as "InActive", update the record 
            update_records = []
            for data in records:
                for res in response:
                    if res["Customer"]["DisplayName"] == data["Customer"]["DisplayName"]:
                        if data["operation"] == "create" and data["Customer"]["Active"] == False:
                            data["operation"] = "update"
                            data["Customer"]["Id"] = res["Customer"]["Id"]
                            update_records.append(data)
            if len(update_records) > 0:
                # Send a separate request for each "Customer" data using map
                update_records = [data["Customer"] for data in update_records]
                response = map(lambda data: self.make_request(url,data), update_records)
                # Handle the response for each request using the handle_response function
                result_update = map(lambda response: self.handle_response(response), list(response))
                # Update the latest state for each successful request
                for state in list(result_update):
                    self.update_state(state)
               
                
            # Update the latest state for each successful request
            for state in list(result):
                self.update_state(state)


        # If the stream is "TaxRate", send a separate request for each record
        elif self.stream_name == "TaxRate":
            # Build the URL to send the requests to
            url = f"{self.base_url}/taxservice/taxcode"
            # Extract the "TaxService" data from each record
            list_data = [data["TaxService"] for data in records]
            # Send a separate request for each "TaxService" data using map
            response = map(lambda data: self.make_request(url,data), list_data)
            # Handle the response for each request using the handle_response function
            result = map(lambda response: self.handle_response(response), list(response))
            # Update the latest state for each successful request
            for state in list(result):
                self.update_state(state)
        else:
            # If the stream is not "TaxRate", send a single batch request for all records
            response = self.make_batch_request(records)
            # Handle the batch response 
            result = self.handle_batch_response(response)
            # Update the latest state for each state update in the response
            for state in result.get("state_updates", list()):
                self.update_state(state)

    
    def make_request(self, url, data):
        access_token = self.access_token

        # Send the request
        r = requests.post(
            url,
            data=json.dumps(data),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}",
            },
        )

        response = r.json()
        self.logger.info(f"DEBUG RESPONSE: {response}")
        return response

    def handle_response(self, response):
        entities = [
            "JournalEntry",
            "Customer",
            "Item",
            "Invoice",
            "CreditMemo",
            "Bill"
        ]
        # If the response has a "Fault" key, it means there was an error
        if response.get("Fault") is not None:
            # Log the error message
            self.logger.error(response)
            # Return a dictionary indicating that the request was not successful,
            # and include the error message in the response
            return {
                "success": False,
                "error": response.get("Fault").get("Error")
            }
        else:
            # If there was no error, return a dictionary indicating that the request
            # was successful, and include the response data
            for entity in entities:
                if not response.get(entity):
                    continue
                record = response.get(entity)
                return {
                    "success": True,
                    "entityData": record,
                    "Id": record.get("Id"),
                }

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
            "Bill",
            "SalesReceipt",
            "Deposits"
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

