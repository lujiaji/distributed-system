import socket
import threading
import json
import time
import hashlib
import random

class Client:
    def __init__(self,servers):
        self.servers = servers
        self.leader_list={
            "cluster1":"",
            "cluster2":"",
            "cluster3":""
            }
        self.eventList = []
        self.twoPCList = []
        self.raftListTime = []
        self.messageTime=[]
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.bind(("127.0.0.1",5010))
        self.client_socket.listen(30)
        self.twoPC_waitTime = []
        self.timeout_limit=1

        threading.Thread(target = lambda : self.listening()).start()
        threading.Thread(target = lambda : self.handleMyEvent()).start()
        threading.Thread(target = lambda : self.monitor_2PC_timeout()).start()

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
                if (msg_data["type"] == "leader"):
                    self.update_leader_list(msg_data)
                if (msg_data["type"] == "abort_raft"):
                    self.sendRaftAbort(msg_data)
                if (msg_data["type"] == "2pc_abort_from_server"):
                    self.send2pcAbort(msg_data,None)
                if (msg_data["type"] == "2pc_can_commit"):
                    self.handle2PCcommit(msg_data)
                if (msg_data["type"] == "this_raft_commited"):
                    for e in self.raftListTime:
                        if e["mid"]==msg_data["mid"]:
                            msg_time = int((time.time() - e["init_time"]) * 1000)
                            self.messageTime.append(msg_time)
                            self.raftListTime.remove(e)
                    print("commit")
    
    def crashServer(self,client_id):
        msg = {
            "type": "crash",
            "client_id": client_id
        }
        msg = json.dumps(msg)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(("127.0.0.1",int("500"+client_id)))
        client_socket.send(msg.encode())
        client_socket.close()

    def recoverServer(self,client_id):
        msg = {
            "type": "recover",
            "client_id": client_id
        }
        msg = json.dumps(msg)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(("127.0.0.1",int("500"+client_id)))
        client_socket.send(msg.encode())
        client_socket.close()

    def partitionServer(self,client_id):
        msg = {
            "type": "partition",
            "client_id": client_id
        }
        msg = json.dumps(msg)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(("127.0.0.1",int("500"+client_id)))
        client_socket.send(msg.encode())
        client_socket.close()
    
    def partitionCluster(self,client_id):
        this_port = None
        if client_id == "1":
            this_port = self.leader_list["cluster1"]
        if client_id == "2":
            this_port = self.leader_list["cluster2"]
        if client_id == "3":
            this_port = self.leader_list["cluster3"]
        msg = {
            "type": "partition_cluster",
            "client_id": client_id
        }
        msg = json.dumps(msg)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(("127.0.0.1",int(this_port)))
        client_socket.send(msg.encode())
        client_socket.close()

    def recoverCluster(self,client_id):
        this_port = None
        if client_id == "1":
            this_port = self.leader_list["cluster1"]
        if client_id == "2":
            this_port = self.leader_list["cluster2"]
        if client_id == "3":
            this_port = self.leader_list["cluster3"]
        msg = {
            "type": "recover_cluster",
            "client_id": client_id
        }
        msg = json.dumps(msg)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(("127.0.0.1",int(this_port)))
        client_socket.send(msg.encode())
        client_socket.close()

    def update_leader_list(self, msg_data):
        if msg_data["cluster"] == 1:
            self.leader_list["cluster1"] = int(msg_data["leader_port"])
        if msg_data["cluster"] == 2:
            self.leader_list["cluster2"] = int(msg_data["leader_port"])
        if msg_data["cluster"] == 3:
            self.leader_list["cluster3"] = int(msg_data["leader_port"])
        print(self.leader_list)

    def initTransactionMessage(self,command):
        if len(command) != 3:
            print("Invalid command")
            return
        else:
            transaction_sourse = int(command[0])
            transaction_target = int(command[1])
            transaction_amount = int(command[2])
        if transaction_sourse not in range(1,3001) or transaction_target not in range(1,3001):
            print("Invalid customer ID")
            return
        if transaction_amount < 0:
            print("Invalid transaction amount")
            return
        data = {
            "type": "",
            "transaction_sourse": transaction_sourse,
            "transaction_target": transaction_target,
            "transaction_amount": transaction_amount,
            "command": command,
            "mid":hashlib.md5(str(random.random()).encode()).hexdigest()
        }
        time.sleep(0.5)
        self.eventList.append(data)
        
    def handle2PCcommit(self, msg_data):
        this_event = {}
        for i in self.twoPCList:
            if i["mid"] == msg_data["mid"]:
                this_event = i
        if this_event == {}:
            print("handle2PCcommit No event found")
            return
        if (msg_data["transaction_sourse_can"] == 1):
            this_event["transaction_sourse_can"] = 1
        if (msg_data["transaction_target_can"] == 1):
            this_event["transaction_target_can"] = 1
        if (this_event["transaction_sourse_can"] == 1 and this_event["transaction_target_can"] == 1):
            print("do_commit")
            msg = {
                "type": "2pc_commit",
                "transaction_sourse": this_event["transaction_sourse"],
                "transaction_target": this_event["transaction_target"],
                "transaction_amount": this_event["transaction_amount"],
                "command": this_event["command"],
                "mid": this_event["mid"]
            }
            msg = json.dumps(msg)
            for this_server in self.servers:
                if this_server["cluster"]==this_event["this_sourse_cluster"] or this_server["cluster"]==this_event["this_target_cluster"]:
                    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client_socket.connect(("127.0.0.1",int(this_server["port"])))
                    client_socket.send(msg.encode())
                    client_socket.close()
            self.twoPCList.remove(this_event)
            for i in self.twoPC_waitTime:
                if i["mid"] == msg_data["mid"]:
                    msg_time = int((time.time() - i["time"]) * 1000)
                    self.messageTime.append(msg_time)
                    self.twoPC_waitTime.remove(i)

    def handleMyEvent(self):
        while True:
            time.sleep(0.8)
            if not self.eventList or self.eventList[0] is None:
                continue
            if not self.eventList or self.eventList[0]["type"] == "2PC":
                continue

            if 1 <= self.eventList[0]["transaction_sourse"]  <= 1000:
                this_sourse_port = self.leader_list["cluster1"]
                this_sourse_cluster = 1
            elif 1001 <= self.eventList[0]["transaction_sourse"]  <= 2000:
                this_sourse_port = self.leader_list["cluster2"]
                this_sourse_cluster = 2
            elif 2001 <= self.eventList[0]["transaction_sourse"]  <= 3000:
                this_sourse_port = self.leader_list["cluster3"]
                this_sourse_cluster = 3
            
            if 1 <= self.eventList[0]["transaction_target"]  <= 1000:
                this_target_port = self.leader_list["cluster1"]
                this_target_cluster = 1
            elif 1001 <= self.eventList[0]["transaction_target"]  <= 2000:
                this_target_port = self.leader_list["cluster2"]
                this_target_cluster = 2
            elif 2001 <= self.eventList[0]["transaction_target"]  <= 3000:
                this_target_port = self.leader_list["cluster3"]
                this_target_cluster = 3

            if(this_sourse_port == "" or this_target_port == ""):
                print("No leader found")
                continue

            if(this_sourse_port == this_target_port):
                msg = {
                    "type": "init_Raft",
                    "transaction_sourse": self.eventList[0]["transaction_sourse"],
                    "transaction_target": self.eventList[0]["transaction_target"],
                    "transaction_amount": self.eventList[0]["transaction_amount"],
                    "command": self.eventList[0]["command"],
                    "mid": self.eventList[0]["mid"]
                }
                self.raftListTime.append({"mid":msg["mid"],"init_time":time.time()})
                msg = json.dumps(msg)
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect(("127.0.0.1",this_sourse_port))
                client_socket.send(msg.encode())
                client_socket.close()
                print("init_Raft sent")
                self.eventList.pop(0)
            else:
                msg = {
                    "type": "init_2PC",
                    "transaction_sourse": self.eventList[0]["transaction_sourse"],
                    "transaction_sourse_can": 0,
                    "transaction_sourse_cluster": this_sourse_cluster,
                    "transaction_target": self.eventList[0]["transaction_target"],
                    "transaction_target_can": 0,
                    "transaction_target_cluster": this_target_cluster,
                    "transaction_amount": self.eventList[0]["transaction_amount"],
                    "command": self.eventList[0]["command"],
                    "mid": self.eventList[0]["mid"]
                }
                self.eventList[0]["this_sourse_port"]=this_sourse_port
                self.eventList[0]["this_sourse_cluster"]=this_sourse_cluster
                self.eventList[0]["this_target_port"]=this_target_port
                self.eventList[0]["this_target_cluster"]=this_target_cluster
                self.eventList[0]["transaction_sourse_can"]=0
                self.eventList[0]["transaction_target_can"]=0
                self.twoPC_waitTime.append({"mid":msg["mid"],"time":time.time()}) 
                msg = json.dumps(msg)
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect(("127.0.0.1",this_sourse_port))
                client_socket.send(msg.encode())
                client_socket.close()
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect(("127.0.0.1",this_target_port))
                client_socket.send(msg.encode())
                client_socket.close()
                self.eventList[0]["type"] = "2PC"
                self.eventList[0]["init_time"] = time.time()
                self.twoPCList.append(self.eventList[0])
                self.eventList.pop(0)

    def monitor_2PC_timeout(self):
        while True:
            if self.twoPC_waitTime != []:
                time.sleep(0.1)
                for i in self.twoPC_waitTime:
                    time_gap=time.time()-i["time"]
                    if time_gap>self.timeout_limit:
                        msg_data = {}
                        mid = i["mid"]
                        self.send2pcAbort(msg_data,mid)
    def sendRaftAbort(self,msg_data):
        this_event = {}
        this_mid = ""
        for i in self.raftListTime:
            if "mid" in msg_data and i["mid"] == msg_data["mid"]:
                this_event = i
                this_mid = i["mid"]

        if this_event == {}:
            return
        self.raftListTime.remove(this_event)
        print("------------------Client side RaftAbort------------------")

    def send2pcAbort(self,msg_data, mid):
        this_event = {}
        this_mid = ""
        for i in self.twoPCList:
            if mid != None and i["mid"] == mid:
                this_event = i
                this_mid = i["mid"]
            else:
                if msg_data != {} and i["mid"] == msg_data["mid"]:
                    this_event = i
                    this_mid = i["mid"]

        if this_event == {}:
            print("send2pcAbort No event found")
            return
        print("------------------Client side 2pc Abort------------------")
        msg = {
                "type": "2pc_abort",
                "transaction_sourse": this_event["transaction_sourse"],
                "transaction_target": this_event["transaction_target"],
                "transaction_amount": this_event["transaction_amount"],
                "command": this_event["command"],
                "mid": this_event["mid"]
            }
        msg = json.dumps(msg)
        if msg_data == {}:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(("127.0.0.1",this_event["this_sourse_port"]))
            client_socket.send(msg.encode())
            client_socket.close()
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(("127.0.0.1",this_event["this_target_port"]))
            client_socket.send(msg.encode())
            client_socket.close()
        else:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if (int(msg_data["abort_server"]) == int(this_event["this_sourse_port"])):
                client_socket.connect(("127.0.0.1",int(this_event["this_target_port"])))
            if (int(msg_data["abort_server"]) == int(this_event["this_target_port"])):
                client_socket.connect(("127.0.0.1",int(this_event["this_sourse_port"])))
            client_socket.send(msg.encode())
            client_socket.close()
        self.twoPCList.remove(this_event)
        for i in self.twoPC_waitTime:
            if i["mid"] == this_mid:
                self.twoPC_waitTime.remove(i)
