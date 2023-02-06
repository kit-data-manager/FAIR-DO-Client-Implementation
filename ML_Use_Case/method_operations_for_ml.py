import logging
import json
import os
import urllib.request
import hashlib
import ast
import glob
from Native_Client.method_operations import Method_Operations

class Method_Operations_For_ML(Method_Operations):

    def __init__(self, config):
        super().__init__(config)

    def operation_get_location(self, pid_record):
        super(Method_Operations_For_ML, self).operation_get_location()

    def operation_get_mime_type(self, pid_record):
        super(Method_Operations_For_ML, self).operation_get_mime_type()

    def operation_evaluate_license(self, pid_record, license_criteria:list):
        super(Method_Operations_For_ML, self).operation_evaluate_license()

    def operation_get_metadata(self, pid_record):
        super(Method_Operations_For_ML, self).operation_get_metadata()

    def operation_evaluate_topic(self, pid_record, topics_criteria:list):
        match=0
        try:
            for topic in pid_record["entries"][self.TOPICS]:
                if topic["value"] in topics_criteria:
                    match+=1
            if match>0:
                return True
            else:
                return False
        except KeyError:
            logging.warning("no topics attribute provided")
            return False

    def operation_evaluate_schema(self, type_record, schemas_criteria:list):
        try:
            if type_record["entries"][self.SCHEMA][0]["value"] in schemas_criteria:
                return type_record["entries"][self.SCHEMA][0]["value"]
            else:
                return False
        except KeyError:
            logging.warning("no schema attribute provided")
            return False

    def operation_get_label(func):
        def inner(self, uri):
            with urllib.request.urlopen(uri, context=self.ctx) as response:
                label_document= response.read()
            json_label_document = json.loads(label_document)
            label_term=func(self, json_label_document)
            return label_term
        return inner
        
    @operation_get_label
    def operation_get_label_mlbasicschema(self, file_content):
        term_specifications_file=file_content["labelProperties"][0]["labelTerm"]["termDefinition"]
        return term_specifications_file

    @operation_get_label
    def operation_get_label_definition(self, json_schema):
        for key in json_schema["graph"]:
            if "dct:modified" in list(key.keys()):
                concept=key["uri"]
                try:
                    relatedConcepts=[]
                    if type(key["related"])==type(list()):
                        for subkey in key["related"]:
                            relatedConcepts=relatedConcepts+[subkey["uri"]]
                    else:
                        relatedConcepts+[key["related"]["uri"]]
                except:
                    pass
                try:
                    narrowerConcepts=[]
                    if type(key["narrower"])==type(list()):
                        for subkey in key["narrower"]:
                            narrowerConcepts=narrowerConcepts+[subkey["uri"]]
                    else:
                        narrowerConcepts+[key["narrower"]["uri"]]
                except KeyError:
                    pass
                try:
                    broaderConcepts=[]
                    if type(key["broader"])==type(list()):
                        for subkey in key["broader"]:
                            broaderConcepts=broaderConcepts+[subkey["uri"]]
                    else:
                        narrowerConcepts+[key["broader"]["uri"]]
                except KeyError:
                    pass
        return {concept: {"relatedConcepts": relatedConcepts, "narrowerConcepts": narrowerConcepts, "broaderConcepts": broaderConcepts}}

    def operation_get_data(self, pid_record):
        try:
            os.makedirs(self.DIRECTORY+"/digital_objects/"+pid_record["pid"])
            dir=self.DIRECTORY+"/digital_objects/"+pid_record["pid"]
            for location in pid_record["entries"][self.LOCATION]:
                file=self.DIRECTORY+"/digital_objects/"+pid_record["pid"]+"/data_object."+location["value"].split('.')[-1]
                response=urllib.request.urlopen(pid_record["entries"][self.LOCATION][0]["value"], context=self.ctx)
                data=response.read()
                with open(file, "wb") as opfile:
                    opfile.write(data)
        except FileExistsError:
            logging.warning("files already downloaded")
            dir=self.DIRECTORY+"/digital_objects/"+pid_record["pid"]
            pass
        except KeyError:
            logging.warning("no location attribute provided")
            return False
        return dir, pid_record["pid"]

    def operation_evaluate_checksum(self, pid_record, files):
        try:
            pid_record_checksum=ast.literal_eval(pid_record["entries"][self.CHECKSUM][0]["value"])
            match list(pid_record_checksum.keys())[0]:
                case "sha512sum":
                    hash_from_record=list(pid_record_checksum.values())[0]
                    hash = hashlib.sha512()
        except KeyError:
            logging.warning("no checksum attribute provided")

        for filename in glob.glob(files+"/*"):
            with open(filename, "rb") as file:
                for byte_block in iter(lambda: file.read(4096),b""):
                    hash.update(byte_block)
                if hash.hexdigest() == hash_from_record:
                    return True
                else:
                    pid=pid_record["pid"]
                    logging.warning(f"for {pid} the checksum is not correct")
                    return False
        