import logging
import json
import ssl
from abc import ABC, abstractmethod

class Method_Operations():

    def __init__(self, config: json = "client_config.json") -> None:

        with open(config, "rb") as open_config:
            json_config = json.loads(open_config.read())

        self.MIME_TYPE=json_config["mime_type"]
        self.LICENSE=json_config["license"]
        self.TOPICS=json_config["topics"]
        self.METADATA=json_config["metadata"]
        self.SCHEMA=json_config["schema"]
        self.LOCATION=json_config["location"]
        self.CHECKSUM=json_config["checksum"]
        self.DIRECTORY=json_config["directory"]

        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE

    @abstractmethod
    def operation_get_location(self, pid_record):
        try:
            return pid_record["entries"][self.LOCATION][0]["value"]
        except KeyError:
            logging.warning("no location attribute provided")
            return False

    @abstractmethod
    def operation_get_mime_type(self, pid_record):
        try:
            return pid_record["entries"][self.MIME_TYPE][0]["value"]
        except KeyError:
            logging.warning("no type attribute provided")
            return False

    @abstractmethod
    def operation_evaluate_license(self, pid_record, license_criteria:list):
        try:
            if pid_record["entries"][self.LICENSE][0]["value"] in license_criteria:
                return True
            else:
                return False
        except KeyError:
            logging.warning("no license attribute provided")
            return False

    @abstractmethod
    def operation_get_metadata(self, pid_record):
        try:
            metadata_FAIRDOs=[]
            for md_pid in pid_record["entries"][self.METADATA]:
                metadata_FAIRDOs.append(md_pid["value"])
            return metadata_FAIRDOs
        except KeyError:
            logging.warning("no metadata attribute provided")
            return False