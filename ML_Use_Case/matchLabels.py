import json

class MatchLabels():

    def __init__(self):
        self.extracted_concepts={}

    def find_concepts(self, concept_items, pids=[]):
        
        for key, value in concept_items.items():
            if list(value.keys())[0].startswith("http"):
                pids.append(key)
                self.set_concepts(pids, list(value.keys())[0], value[list(value.keys())[0]])
                pids=[]
            else:
                pids.append(key)
                self.find_concepts(value, pids)
                pids=[]
        return None

    def set_concepts(self, pids, concept, conceptItems):
        concept_list=[]
        for key in list(conceptItems.keys()):
            if isinstance(conceptItems[key], dict):
                self.get_concepts(conceptItems[key])
            else:
                concept_list.extend(conceptItems[key])
        self.extracted_concepts[json.dumps(pids)]=(concept, concept_list)
        return None

    def get_concepts(self):
        return self.extracted_concepts

    @classmethod
    def relate_concepts(cls, all_concepts):
        pids_concepts={}
        similar_concepts=[]
        related_pids=[]
        rel_nar_bro_concept=[]
        former_concept=None
        former_pid=None
        for key, value in all_concepts.items():
            if (value[0]==former_concept) or (value[0] in rel_nar_bro_concept):
                similar_concepts.append(former_concept)
                similar_concepts.append(value[0])
                related_pids.append(json.loads(former_pid))
                related_pids.append(json.loads(key))
                pids_concepts[json.dumps(related_pids)]=similar_concepts
            else:
                pass
            rel_nar_bro_concept=value[1]
            former_concept=value[0]
            former_pid=key
            related_pids=[]
            similar_concepts=[]
        return pids_concepts

    @classmethod
    def merge_concepts(cls, pids_concepts):
        former_value=[]
        former_key=[]
        new_pids_concepts={}
        for keys, values in pids_concepts.items():
            if any(value in values for value in former_value):
                new_pids_concepts[json.dumps(list(set(json.loads(keys)) | set(json.loads(former_key))))]=list(set(values) | set(former_value))
                former_value=values
                former_key=keys
            else:
                former_value=values
                former_key=keys
        if len(new_pids_concepts)==0:
            return new_pids_concepts
        else:
            new_pids_concepts=cls.merge_concepts(new_pids_concepts)
            return new_pids_concepts
