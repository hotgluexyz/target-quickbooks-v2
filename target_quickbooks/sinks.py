from contextlib import redirect_stderr
from os import environ
import requests
import json
from singer_sdk.sinks import BatchSink
from datetime import datetime
from intuitlib.client import AuthClient
from intuitlib.enums import Scopes
from singer_sdk.plugin_base import PluginBase
from typing import Any, Dict, List, Mapping, Optional, Union

class QuickBooksSink(BatchSink):
    """QuickBooks target sink class."""
    def __init__(self, target: PluginBase, stream_name: str, schema: Dict, key_properties: Optional[List[str]]) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        # Instanciate Client
        self.last_refreshed = None
        self.config_file = target.config_file
        self.access_token = self.config.get("access_token")
        self.refresh_token = self.config.get("refresh_token")
        self.instanciate_client() 
        if self.config.get("is_sandbox"):
            self.base_url = "https://sandbox-quickbooks.api.intuit.com"
        else:
            self.base_url = "https://quickbooks.api.intuit.com"

    max_size = 100  # Max records to write in one batch

    def instanciate_client(self):
        client_id = self.config.get("client_id")
        client_secret = self.config.get("client_secret")
        redirect_uri = self.config.get("redirect_uri")

        if self.config.get("is_sandbox"):
            environment = "sandbox"
        else:
            environment = "production"

        self.auth_client = AuthClient(client_id,client_secret,redirect_uri,environment)

        if not self.is_token_valid():
            self.update_access_token()

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
        if round(datetime.now().timestamp()) - last_update > 3300: # 1h - 5min
            return False 
        return True

    @property
    def authenticator(self):
        auth_credentials = {"Authorization": f"Bearer {self.access_token}"}
        return auth_credentials

    @property
    def get_url_params(self):
        params = {}
        params["minorversion"] = 40 #minorversion=40
        return params

    def start_batch(self, context: dict) -> None:
        
        if not self.is_token_valid():
            self.update_access_token()

        if self.stream_name == "JournalEntries":
            realm = self.config.get("realmId")
            context["url"] = f"{self.base_url}/v3/company/{realm}/journalentry"
    
    def get_entities(self,entity_type, key="Name", fallback_key="Name", check_active=True):
        access_token = self.access_token
        offset = 0
        max = 100
        entities = {}

        while True:
            query = f"select * from {entity_type}"
            if check_active:
                query = query + " where Active=true"
            realmId = self.config.get("realmId")
            query = query + f" STARTPOSITION {offset} MAXRESULTS {max}"
            url = f"{self.base_url}/v3/company/{realmId}/query?query={query}&minorversion=40"
            
            #logger.info(f"Fetch {entity_type}; url={url}; query {query}")
            
            r = requests.get(url, headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {access_token}'
            })

            response = r.json()

            # Establish number of records returned.
            count = response['QueryResponse'].get('maxResults')

            # No results - exit loop.
            if not count or count == 0:
                break

            # Parse the results
            records = response['QueryResponse'][entity_type]

            if not records:
                records = []

            # Append the results
            for record in records:
                entity_key = record.get(key, record.get(fallback_key))
                # Ignore None keys
                if entity_key is None:
                    #logger.warning(f"Failed to parse record f{json.dumps(record)}")
                    continue

                entities[entity_key] = record

            # We're done - exit loop
            if count < max:
                break

            offset += max

        #logger.debug(f"[get_entities]: Found {len(entities)} {entity_type}.")

        return entities

    def process_record(self, record: dict, context: dict) -> None:
        accounts = self.get_entities("Account", key="AcctNum")
        if not context.get("records"):
            context["records"] = []
        context["records"].append(record)

    def process_batch(self, context: dict) -> None:
        # if not context.get("url"):
        #     self.logger.warning(f"Stream {self.stream_name} not supported.")

        # if len(context.get("records")):
        #     # not working
        #     # self.logger.info(f"Updating {self.stream_name} data")
        #     # response = requests.put(context["url"], headers=self.authenticator, json=context["records"],params=self.get_url_params)
        #     # for status in response.json():
        #     #     if status.get("success"):
        #     #         self.logger.info(f"Reference {status.get('code')} updated succesfuly")
        #     #     elif status.get("success")==False:
        #     #         self.logger.warning(f"It was not possible to update index {status.get('index')}: {status.get('errors')}")
        pass

