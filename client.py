import socket
import threading
import json


class Client:
    def __init__(self):
        self.leader_list=[]
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.bind(("127.0.0.1",5010))
        self.client_socket.listen(30)
        threading.Thread(target = lambda : self.listening()).start()
        print("Client started")
    def send(self, data):
        self.client_socket.send(data)
    
    def listening(self):
        while True:
            client_socket, address = self.client_socket.accept()
            while True:
                data = client_socket.recv(1024).decode()
                if not data:
                    break
                msg_data = json.loads(data)
                if (msg_data["code"] == "check"):
                  print("check")
                if (msg_data["code"] == "check_rply"):
                   print("check_rply")
                if (msg_data["code"] == "release"):
                    print("release")

    def initTransactionMessage(self,command):
        if self.leader_list == []:
            print("No leader")
            return
        elif len(command) > 3:
            command_target = command[0]
        print("initTransactionMessage")
        data = {
            "code": "init",
            "command": command
        }
        self.send(json.dumps(data).encode())
        print("initTransactionMessage sent")