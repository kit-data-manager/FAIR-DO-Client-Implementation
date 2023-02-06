from Native_Client.client_coordinator import Client_Coordinator
from retrieve_fair_do_for_ml import Retrieve_FAIR_DO_for_ML
from matchLabels import MatchLabels
from method_operations_for_ml import Method_Operations_For_ML
import json

import logging

class Client_Coordinator_For_ML(Client_Coordinator):

    def __init__(self, search_config, client_config):
        super().__init__(search_config, client_config)
        self.methods_class=Method_Operations_For_ML(self.client_config)

    def orchestration(self):
        retrieve_fairdo=Retrieve_FAIR_DO_for_ML(self.client_config, self.methods_class, **self.search_metadata)
        all_return_values={}
        #implement multithreading
        for pid in self.pid_list:
            return_value=retrieve_fairdo.type_based_operations(pid)
            all_return_values[pid]=return_value

        matched_labels=MatchLabels()
        matched_labels.find_concepts(all_return_values)
        pids_concepts=matched_labels.relate_concepts(matched_labels.get_concepts())
        logging.info(f"relabeld terms are: {pids_concepts}")
        for keys in list(pids_concepts.keys()):
            for pids in json.loads(keys):
                retrieve_fairdo.data_operations(pids)
