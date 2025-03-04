from tinydb import TinyDB, Query
import threading
import socket
import json
import time
import hashlib
import random
# "server_info":{
#         "server_id": "S1",
#         "cluster": 1,
#         "ip": "127.0.0.1",
#         "port": "5001",
#         "db_file": "data/db_server_S1.json"
#     }
class Server:
    def __init__(self, server_info,servers):
        # Initialize some parameters
        self.server_id = server_info["server_id"]
        self.cluster = server_info["cluster"]  # Needs to be initialized
        self.ip = server_info["ip"]
        self.port = server_info["port"]
        self.db_file = server_info["db_file"]
        self.others=[]

        for server_info in servers:
            if server_info["server_id"]!=self.server_id and server_info["cluster"]==self.cluster:
                self.others.append(server_info)
        #print(f"{self.server_id}:{self.others}")
        self.data_base = self.initMyStorage(self.db_file)
        self.data_db = self.data_base.table('data')
        self.log_db = self.data_base.table('logs')  # Needs to be initialized

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip,int(self.port)))
        self.socket.listen(30)
        self.leader = None
        self.timeout_limit=random.uniform(2, 3)
        self.last_message_time=time.time()
        self.leader_exist=0
        self.pending_elec={}
        self.elec_id=None

        threading.Thread(target = lambda : self.as_leader_hb()).start()
        threading.Thread(target = lambda : self.monitor_timeout()).start()
        threading.Thread(target = lambda : self.listening()).start()

        
        #------------ 以上已完成初始化，以下是需要实现的 ------------

        self.mode = None  # "election", "normal", "C_change"
        # Persistent state, update in Storage before replying to RPC
        self.cur_term = 0  # Monotonically increase, or sync with current leader
        #self.leader = None  # To vote and remember current leader
        # Volatile state on all servers
        self.commit_index = [0, 0]  # Committed entry index
        # Volatile state on leaders, reset after each election (specifically index)
        self.cur_index = [0, 0]  # [term, index]
        self.match_index = [0, 0]  # Already replicated on server
        self.log_temp = None # 临时存储log

        
    def as_leader_hb(self):
        while True:
            time.sleep(0.2)
            if self.leader==self.server_id:
                print(f"[{self.server_id}] send heartbeat")
                message={
                    "type":"hb",
                    "leader_id":self.server_id,
                    "ip":self.ip,
                    "port":self.port
                }
                message=json.dumps(message)
                for other in self.others:
                    my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    my_socket.connect((other["ip"],int(other["port"])))
                    my_socket.send(message.encode())
                    my_socket.close()
            else:
                continue
            
    def listening(self):
        while True:
            client_socket, address = self.socket.accept()
            while True:
                data = client_socket.recv(1024).decode()
                # add delay here
                # mySleep(self.sleepType)
                if not data:
                    break
                else:
                    print(f"[{self.server_id}] listened")
                    self.last_message_time=time.time()
                    msg_data = json.loads(data)
                    match msg_data["type"]:
                        case "hb":
                            print(f"[{self.server_id}] receive hb")
                        case "elec":
                            print(f"[{self.server_id}] receive elec,need to vote!")
                            self.vote(msg_data)
                        case "vote":
                            print(f"[{self.server_id}] receive vote!")
                            self.handle_vote(msg_data)
                    # if (msg_data["code"] == "check_rply"):
                    #     print("check_rply")
                    # if (msg_data["code"] == "release"):
                    #     print("release")


    def initMyStorage(self, db_file_name):
        print("initMyStorage")
        # this is the way to check log in storage
        # dataQuery = Query()
        # logs_result = self.storage.search(dataQuery.customer_id == 2)
        # if len(logs_result) > 0:
        #     print(logs_result[0])
        db = TinyDB(db_file_name)
        return db
    
    """Chain functions according to execution order"""
    def election(self):
        self.leader=None
        self.leader_exist=0
        self.cur_term+=1
        message={
            "type":"elec",
            "candidate_id":self.server_id,
            "last_included_index":self.commit_index,
            "last_included_term":self.cur_term,
            "ip":self.ip,
            "port":self.port,
            "mid":hashlib.md5(str(random.random()).encode()).hexdigest()
            }
        self.elec_id=message["mid"]
        self.pending_elec[message["mid"]]={
            "my_elec_mid":message["mid"],
            "others_replied":[],
            "elec_term":self.cur_term
        }
        self.pending_elec[message["mid"]]["others_replied"].append(True)
        message=json.dumps(message)
        for other in self.others:
            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            my_socket.connect((other["ip"],int(other["port"])))
            my_socket.send(message.encode())
            my_socket.close()

        return

    def monitor_timeout(self):
        while True:
            time.sleep(0.5)
            if (self.leader!=self.server_id):
                time_gap=time.time()-self.last_message_time
                if time_gap>self.timeout_limit:
                    print(f"[{self.server_id}] long time no receive msgs,timeout!")
                    self.leader_exist=0
                    self.leader=None
                    self.election()
            else:
                return

    def vote(self,msg):
        #compare index term 
        if msg["last_included_term"]>self.cur_term:
            self.cur_term=msg["last_included_term"]
            message={
                "type":"vote",
                "voter_id":self.server_id,
                "term":self.cur_term,
                "reply_to_mid":msg["mid"],
                "granted_vote":True
            }
            self.leader=msg["candidate_id"]
            self.leader_exist=1
            message=json.dumps(message)
            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            my_socket.connect((msg["ip"],int(msg["port"])))
            my_socket.send(message.encode())
            print(f"[{self.server_id}] granted {msg["candidate_id"]}'s election with my term:{self.cur_term}!")
            print(f"[{self.server_id}] my leader is {self.leader}")
            my_socket.close()
        elif msg["last_included_term"]==self.cur_term and msg["last_included_index"]>self.commit_index: 
            self.cur_term=msg["last_included_term"]
            message={
                "type":"vote",
                "voter_id":self.server_id,
                "term":self.cur_term,
                "reply_to_mid":msg["mid"],
                "granted_vote":True
            }
            self.leader=msg["candidate_id"]
            self.leader_exist=1
            message=json.dumps(message)
            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            my_socket.connect((msg["ip"],int(msg["port"])))
            my_socket.send(message.encode())
            print(f"[{self.server_id}] granted {msg["candidated_id"]}'s election with my term:{self.cur_term}!")
            my_socket.close()
        else:
            message={
                "type":"vote",
                "voter_id":self.server_id,
                "term":self.cur_term,
                "reply_to_mid":msg["mid"],
                "granted_vote":False
            }
            self.leader=None
            self.leader_exist=0
            message=json.dumps(message)
            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            my_socket.connect((msg["ip"],int(msg["port"])))
            my_socket.send(message.encode())
            print(f"[{self.server_id}] rejected {msg["candidate_id"]}'s election with my term:{self.cur_term}!")
            my_socket.close()
            self.election()

    def handle_vote(self,msg):
        if msg["reply_to_mid"]==self.elec_id:
            print("111")
            if msg["granted_vote"]==True:
                self.pending_elec[self.elec_id]["others_replied"].append(True)
                if len(self.pending_elec[self.elec_id]["others_replied"]) > 1:
                    for i in self.pending_elec[self.elec_id]["others_replied"]:
                        if i == False:
                            print("vote fail")
                            return
                    self.leader=self.server_id
                    self.leader_exist=1
                    if len(self.pending_elec[self.elec_id]["others_replied"])==3:
                        self.pending_elec={}
                        print(f"[{self.server_id}] I become leader!")
                else:
                    print("not enough votes")
                    return
        else:
            return
    # def receiveMsgs(self):
    # """Divided into three situations: "election", "normal", "C_change",
    # Can be distinguished by the outermost key of the msg's dict
    # if (outermost key is "normal"):
    #     switch ("phase"):
    #         case "request":
    #             do sendReply()
    #         case "reply_request":
    #             do sendCommit()
    #         case "committed":
    #             do self.update()
    # """

    # def receiveClientRequest(self):

    # def sendRequest(self):

    def sendReply(self):
        # if (self.storage.valid()):  # Fill it
        #     message = Message({"""Refer to the format of normal-replyRequest"""})
        #     message.sendTo(self.leader)
        return
    # def sendCommit(self):
        # if (committed from majority):
            # message = Message({"""Refer to the format of normal-committed"""})
            # for client in self.cluster:
            #     message.sendTo(client)

    # def update(self):
        # self.storage.update()
