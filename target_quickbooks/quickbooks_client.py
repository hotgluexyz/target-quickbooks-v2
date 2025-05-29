import json
import requests
from datetime import datetime

from intuitlib.client import AuthClient

class QuickbooksClient:
    MINOR_VERSION = "75"

    def __init__(self, config_file_path, logger):
        self.logger = logger
        self._config_file_path = config_file_path
        self.config = self.load_config()
        self.instantiate_client()

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

        self.update_access_token()

    def load_config(self):
        with open(self._config_file_path, "r") as config_file:
            return json.load(config_file)

    @property
    def authenticator(self):
        auth_credentials = {"Authorization": f"Bearer {self.access_token}"}
        return auth_credentials

    def is_token_valid(self):
        last_update = self.config.get("last_update")
        if not last_update:
            return False
        if round(datetime.now().timestamp()) - last_update > 3300:  # 1h - 5min
            return False
        return True

    def update_access_token(self):
        self.auth_client.refresh(self.config.get("refresh_token"))
        self.access_token = self.auth_client.access_token
        self.refresh_token = self.auth_client.refresh_token
        self.config["refresh_token"] = self.refresh_token
        self.logger.info("Updated refresh token: {}".format(self.refresh_token))
        self.config["access_token"] = self.access_token
        self.config["last_update"] = round(datetime.now().timestamp())

        with open(self._config_file_path, "w") as outfile:
            json.dump(self.config, outfile, indent=4)

    @property
    def base_url(self) -> str:
        realm = self.config.get("realmId")

        return (
            f"https://sandbox-quickbooks.api.intuit.com/v3/company/{realm}"
            if self.config.get("is_sandbox")
            else f"https://quickbooks.api.intuit.com/v3/company/{realm}"
        )

    def get_entities(
        self, entity_type, where_filter=None, select_statement="*"
    ):
        offset = 0
        max = 100
        entities = []

        while True:
            query = f"select {select_statement} from {entity_type}"

            if where_filter:
               query = query + f" where {where_filter}" 
                

            query = query + f" STARTPOSITION {offset} MAXRESULTS {max}"

            self.logger.info(f"Fetch {entity_type}; url={self.base_url}; query {query}; minorversion {self.MINOR_VERSION}")
            
            batch_requests = [
                {
                    "Query": query, 
                    "bId": "bid1"
                }
            ]

            responses = self.make_batch_request(batch_requests)
            if not responses:
                return []
            
            response = responses[0]
            if response.get("Fault"):
                self.logger.error(f"Get Entities error: {json.dumps(response['Fault'])}")
            
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
            entities += records

            # We're done - exit loop
            if count < max:
                break

            offset += max

        self.logger.debug(f"[get_entities]: Found {len(entities)} {entity_type}.")

        return entities

    def make_batch_request(self, batch_requests):
        if not self.is_token_valid():
            self.update_access_token()

        response = self.make_request(
            "POST",
            "/batch",
            request_data={"BatchItemRequest": batch_requests},
        )

        if response is None:
            return []
        
        if response.get("Fault") is not None:
            self.logger.error(response)
            return []

        return response.get("BatchItemResponse")

    def make_request(self, method, endpoint, request_data=None, params={}, headers={}):
        if not self.is_token_valid():
            self.update_access_token()

        request_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        if headers:
            request_headers.update(headers)

        request_params = {"minorversion": self.MINOR_VERSION}
        if params:
            request_params.update(params)

        url = f"{self.base_url}{endpoint}"

        # Send the request
        response = requests.request(
            method,
            url,
            data=json.dumps(request_data) if request_data else None,
            headers=request_headers,
            params=params
        )

        success, error_message = self._validate_response(response)

        if not success:
            self.logger.error(f"Request error: {error_message}")
            return

        response_json = response.json()
        self.logger.info(f"Response: {response_json}")
        return response_json
    
    def _validate_response(self, response: requests.Response) -> tuple[bool, str | None]:
        if response.status_code >= 400:
            msg = json.dumps(response.json())
            return False, msg
        else:
            return True, None

   
    # def search_reference_data(self, reference_data, key, value):
    #     return_data = {}
    #     for data in reference_data:
    #         if key in data:
    #             if isinstance(data,dict):
    #                 if data[key] == value:
    #                     return data
    #             elif isinstance(data,str):
    #                 if data==value:
    #                     return data    
    #     return return_data
    
