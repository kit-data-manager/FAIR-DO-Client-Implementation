from abc import ABC, abstractmethod
import json
import logging

from Native_Client.client_commands import Client_Commands

class Retrieve_FAIR_DO(ABC):
    
    def __init__(self, config, func, **kwargs):

        with open(config, "rb") as open_config:
            json_config = json.loads(open_config.read())

        self.PID_SERVICE= json_config["pid_service"]
        self.SSH_KEY=json_config["ssh_key"]
        self.PASSPHRASE=json_config["passphrase"]
        self.SERVER_IP_ADRESS=json_config["server_ip_adress"]
        self.USERNAME=json_config["username"]
        self.DTR= json_config["dtr"]
        self.KIP= json_config["kip"]
        self.TYPE= json_config["type"]
        self.operation_func=func
        self.__dict__.update(kwargs)

        try:
            self.client=Client_Commands(self.SSH_KEY, self.PASSPHRASE, self.SERVER_IP_ADRESS, self.USERNAME)
            self.client.open_client()
        except Exception as e:
            logging.error("No valid Client configuration provided")

    @abstractmethod
    def get_pid_record(self):
        pass

    @abstractmethod
    def validate_pid_record(self):
        pass

    @abstractmethod
    def evaluate_type(self):
        pass

    @abstractmethod
    def type_based_operations(self):
        pass

    @abstractmethod
    def set_temp_record_storage(self):
        pass

    @abstractmethod
    def get_temp_record_storage(self):
        pass

    @abstractmethod
    def data_operations(self):
        pass