from paramiko import SSHClient, AutoAddPolicy, RSAKey
import json

class Client_Commands():
    
    def __init__(self, ssh_key, passphrase, server_ip_address, username):
        self.ssh_key = ssh_key
        self.passphrase=passphrase
        self.server_ip_address=server_ip_address
        self.username=username
        
    def open_client(self):
        self.client = SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(AutoAddPolicy())
        private_key = RSAKey.from_private_key_file(self.ssh_key, self.passphrase)
        self.client.connect(self.server_ip_address, username=self.username, pkey = private_key)
        return
    
    def curl_get(self, arg: str):
        command = "curl -s --request GET %s" %(arg)
        stdin, stdout, stderr = self.client.exec_command(command)
        pid_record=stdout.readlines()[0]
        return json.loads(pid_record)

    def close_client(self):
        self.client.close()
        return