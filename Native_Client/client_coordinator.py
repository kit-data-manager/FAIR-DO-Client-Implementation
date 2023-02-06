from Native_Client.method_operations import Method_Operations
from Native_Client.retreive_FAIR_DO import Retrieve_FAIR_DO
import json
from abc import ABC, abstractmethod

class Client_Coordinator():
    def __init__(self, search_config: str, client_config: str) -> None:

        with open(search_config, "rb") as open_config:
            json_config = json.loads(open_config.read())

        self.pid_list=json_config["pids"]
        self.search_metadata=json_config["metadata"]
        self.client_config=client_config
        self.methods_class=Method_Operations(self.client_config)

    @abstractmethod
    def orchestration(self):
        retrieve_fairdo=Retrieve_FAIR_DO(self.client_config, self.methods_class, **self.search_metadata)