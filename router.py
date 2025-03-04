import socket
import threading
import json

# only for receiving leader election and redirect msg and retrying message
class Router:
    def __init__(self):
        self.leader_list={
            "cluster1":"",
            "cluster2":"",
            "cluster3":""
            } # should be a list of leader port    
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.bind(("127.0.0.1",5011))
        self.client_socket.listen(30)
        threading.Thread(target = lambda : self.listening()).start()
        print("Router started")

    def listening(self):
        while True:
            client_socket, address = self.client_socket.accept()
            while True:
                data = client_socket.recv(1024).decode()
                if not data:
                    break
                msg_data = json.loads(data)
                if (msg_data["code"] == "Leader"):
                    if msg_data["cluster"] == "cluster1":
                        self.leader_list["cluster1"] = msg_data["leader_port"]
                if (msg_data["code"] == "start_2PC"):
                   print("check_rply")
                if (msg_data["code"] == "start_raft"):
                    print("release")
