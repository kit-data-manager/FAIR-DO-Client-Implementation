from _thread import *
import logging
from Native_Client.retreive_FAIR_DO import Retrieve_FAIR_DO

class Retrieve_FAIR_DO_for_ML(Retrieve_FAIR_DO):
    
    def __init__(self, config, func, **kwargs):
        super().__init__(config, func, **kwargs)
        self.temp_storage={}

    def get_pid_record(self, pid: str) -> dict:
        """Merge via TPM"""
       
        pid_record=self.client.curl_get(self.PID_SERVICE+pid)
        return pid_record

    def validate_pid_record(self, pid_record: dict) -> bool:
        kip=pid_record["entries"][self.KIP][0]["value"]
        profile=self.client.curl_get(self.DTR+kip)
        profile_properties=[]
        for property in profile["properties"]:
            profile_properties.append(property["identifier"])
        
        for key in list(pid_record["entries"].keys()):
            if key in profile_properties:
                pass
            else:
                logging.error("not valid, record and profile don't match")
        #logging.info("record and profile match")
        return

    def evaluate_type(self, pid_record: dict):
        """This method assumes that the FAIR DO type has a PID as value which is accessed through the attribute 21.T11148/1c699a5d1b4ad3ba4956 in the information record."""

        if self.TYPE in pid_record["entries"]:
            type_pid=pid_record["entries"][self.TYPE][0]["value"]
        else:
            logging.error("unknown pid for type evaluation")

        type_record=self.get_pid_record(type_pid)
        self.validate_pid_record(type_record)

        """The type can be evaluated by its attributes, which triggers operations that can be carried out on the data. The attributes are expected to refer to 
        a certain PID. For types that are based on a different profile with different attributes, i.e. PIDs, the client might not be able to work with the FAIR DO.
        Only if the required attributes are present, an operation (i.e. method) can be executed. Which particular operations need to be executed depends on the task
        and is specified in the configuration. If a mandatory operation cannot be executed, the FAIR DO is considered useless for the task. Since this is just a reference
        implementation, the attributes and operations are all implemented in this method. However, for larger clients with multiple functionalities, separate methods will
        be implemented that are called depending on the task, e.g. evaluate_type_relabeling."""

        return type_record

    def type_based_operations(self, pid):
        pid_record=self.get_pid_record(pid)
        self.validate_pid_record(pid_record)
        type_record=self.evaluate_type(pid_record)
        mime_type=self.operation_func.operation_get_mime_type(type_record)

        match mime_type:
            case "image/jpeg" | "application/x-tar":
                if self.operation_func.operation_evaluate_license(pid_record, self.license_criteria) == True:
                    pass
                else:
                    logging.warning("license unknown")
                    return False
                if self.operation_func.operation_evaluate_topic(pid_record, self.topics_criteria) == True:
                    pass
                else:
                    logging.warning("topic unknown")
                    return False
                
                self.set_temp_record_storage(pid_record["pid"], pid_record)
                metadata_fairdo_list=self.operation_func.operation_get_metadata(pid_record)
                all_return_values={}
                for metadata_fairdo in metadata_fairdo_list:
                    return_value=self.type_based_operations(metadata_fairdo)
                    all_return_values[metadata_fairdo]=return_value

                return all_return_values
                
            case "application/json":
                type_schema=self.operation_func.operation_evaluate_schema(type_record, self.schemas_criteria)
                match self.schemas_criteria[type_schema]:
                    case "ml_basic_schema":
                        term_specifications_file=self.operation_func.operation_get_label_mlbasicschema(self.operation_func.operation_get_location(pid_record))
                    #Implementation of other schemas is possible
                #Just for testing:
                term_specifications_file="https://vocabularies.unesco.org/browser/rest/v1/thesaurus/data?uri=http%3A%2F%2Fvocabularies.unesco.org%2Fthesaurus%2Fconcept219&format=application/ld%2Bjson"
                label_concepts=self.operation_func.operation_get_label_definition(term_specifications_file)
                return label_concepts    

    def set_temp_record_storage(self, pid, pid_record):
        self.temp_storage.update({pid: pid_record})
        return

    def get_temp_record_storage(self):
        return self.temp_storage

    def data_operations(self, pids: list):
        temp_rec=self.get_temp_record_storage()
        for pid in pids:
            if pid in temp_rec:
                pid_record=temp_rec[pid]
                dir, pid=self.operation_func.operation_get_data(pid_record)
                self.operation_func.operation_evaluate_checksum(pid_record, dir)
            else:
                pass
        return
