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
        self.server_info = server_info
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
        self.msg_q=[]

        threading.Thread(target = lambda : self.as_leader_hb()).start()
        threading.Thread(target = lambda : self.monitor_timeout()).start()
        threading.Thread(target = lambda : self.listening()).start()

        
        #------------ 以上已完成初始化，以下是需要实现的 ------------

        self.mode = None  # "election", "normal", "C_change"
        # Persistent state, update in Storage before replying to RPC
        self.cur_term = 0  # Monotonically increase, or sync with current leader
        # Volatile state on all servers
        self.commit_index = [0, 0]  # Committed entry index
        # Volatile state on leaders, reset after each election (specifically index)
        self.cur_index = [0, 0]  # [term, index]
        self.match_index = [0, 0]  # Already replicated on server
        self.log_temp = None # 临时存储log

        
    def as_leader_hb(self):
        while True:
            time.sleep(0.5)
            if self.leader==self.server_id:
                #print(f"[{self.server_id}] send heartbeat")
                if len(self.msg_q)==0:
                    message={
                        "type":"hb",
                        "leader_id":self.server_id,
                        "ip":self.ip,
                        "port":self.port
                    }
                    message.update(self.server_info)
                    message=json.dumps(message)
                    for other in self.others:
                        my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                        my_socket.connect((other["ip"],int(other["port"])))
                        my_socket.send(message.encode())
                        my_socket.close()
                else:
                    my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    for index in range(len(self.msg_q)):
                        msg=self.msg_q(index)
                        msg=json.dumps(msg)
                        for other in self.others:
                            my_socket.connect((other["ip"],int(other["port"])))
                            my_socket.send(message.encode())
                            time.sleep(0.1)
                    my_socket.close()
                    self.msg_q=[]
            else:
                continue
            
    def listening(self):
        while True:
            client_socket, address = self.socket.accept()
            while True:
                data = client_socket.recv(1024).decode()
                if not data:
                    break
                else:
                    # print(f"[{self.server_id}] listened")
                    self.last_message_time=time.time()
                    msg_data = json.loads(data)
                    match msg_data["type"]:
                        # case "hb":
                        #     print(f"[{self.server_id}] receive hb")
                        case "elec":
                            print(f"[{self.server_id}] receive elec,need to vote!")
                            self.vote(msg_data)
                        case "vote":
                            print(f"[{self.server_id}] receive vote!")
                            self.handle_vote(msg_data)
                        case "init_Raft":
                            print(f"[{self.server_id}] receive in-cluster client transaction request, handling!")
                            self.request_raft(msg_data)
                        case "init_2PC":
                            print(f"[{self.server_id}] receive x-cluster client transaction request, handling!")
                            self.request_2pc(msg_data)
                        case "request":
                            print(f"[{self.server_id}] receive commit request")
                            self.reply()
                        case "reply":
                            print(f"[{self.server_id}] receive reply")
                            self.valid_commit(msg_data)
                        case "commit":
                            print(f"[{self.server_id}] receive committed")
                            self.committed(msg_data)
            
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
            "last_included_index":self.cur_index,
            "last_included_term":self.cur_term,
            "ip":self.ip,
            "port":self.port,
            "mid":hashlib.md5(str(random.random()).encode()).hexdigest()
            }
        message.update(self.server_info)
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
            message.update(self.server_info)
            self.leader=msg["candidate_id"]
            self.leader_exist=1
            message=json.dumps(message)
            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            my_socket.connect((msg["ip"],int(msg["port"])))
            my_socket.send(message.encode())
            print(f"[{self.server_id}] granted {msg["candidate_id"]}'s election with my term:{self.cur_term}!")
            print(f"[{self.server_id}] my leader is {self.leader}")
            my_socket.close()
        elif msg["last_included_term"]==self.cur_term and msg["last_included_index"]>self.cur_index: 
            self.cur_term=msg["last_included_term"]
            message={
                "type":"vote",
                "voter_id":self.server_id,
                "term":self.cur_term,
                "reply_to_mid":msg["mid"],
                "granted_vote":True
            }
            message.update(self.server_info)
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
                "granted_vote":False,
                "leader_id":self.leader
            }
            message.update(self.server_info)
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
        if msg["reply_to_mid"]==self.elec_id and self.pending_elec!={}:
            #print("111")
            if msg["granted_vote"]==True:
                self.pending_elec[self.elec_id]["others_replied"].append(True)
                if len(self.pending_elec[self.elec_id]["others_replied"]) > 1:
                    for i in self.pending_elec[self.elec_id]["others_replied"]:
                        if i == False:
                            print("vote fail")
                            self.leader=msg["leader_id"]
                            self.leader_exist=1
                            return
                    self.leader=self.server_id
                    self.leader_exist=1

                    #广播给client
                    # 为什么下面是3？？？
                    # if len(self.pending_elec[self.elec_id]["others_replied"])==3:
                    #     self.pending_elec={}
                    #     self.cur_index[0]=self.cur_term
                    #     print(f"[{self.server_id}] I become leader!")
                    self.pending_elec={}
                    self.cur_index[0]=self.cur_term
                else:
                    print("not enough votes")
                    return
        else:
            return
    def request_raft(self,msg):
        x=msg["transaction_sourse"]
        y=msg["transaction_target"]
        amt=msg["transaction_amount"]
        balance["x"]-=amt
        if balance["x"] > 0:#!!!!!!!!!需要实现！！！！！！！！！！！
            self.cur_index[0]=self.cur_term
            self.cur_index[1]+=1
            message={
                "type":"request",
                "leader_id":self.server_id,
                "last_included_index":self.commit_index,
                "last_included_term":self.cur_term,
                "cur_index":self.cur_index,
                "ip":self.ip,
                "port":self.port,
                "x":x,
                "y":y,
                "amt":amt,
                "mid":msg["mid"]
                }
            message.update(self.server_info)
            self.msg_q.append(message)
        else:
            message={
                "type":"abort_raft",
                "leader_id":self.server_id,
                "ip":self.ip,
                "port":self.port,
                "x":x,
                "y":y,
                "amt":amt,
                "mid":msg["mid"]
                }
            message.update(self.server_info)
            message=json.dumps(message)
            #发回给router，中断
    def request_2pc(self,msg):
        return
    # def receiveClientRequest(self):

    # def sendRequest(self):

    def reply(self,msg):
        x=msg["x"]
        y=msg["y"]
        amt=msg["amt"]
        try:
            if msg["cur_index"](0)>=self.cur_term and msg["cur_index"](1)>=self.cur_index:
                #判断term 和index
                if msg["last_included_index"]==self.match_index(1):
                    #！！判断上一个entry是否相同，若hb强制能更新至一致的状态，则这个判断能省略！！
                    balance["x"]-=amt
                    if balance["x"] > 0 > 0:#!!!!!!!!!需要实现！！！！！！！！！！！
                        #最后判断余额
                        self.cur_index[0]=self.cur_term
                        self.cur_index[1]+=1
                        message={
                            "type":"reply",
                            "leader_id":self.server_id,
                            "ip":self.ip,
                            "port":self.port,
                            "commit_status":True,
                            "mid":msg["mid"]
                            }
                        message.update(self.server_info)
                        message=json.dumps(message)
                        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client_socket.connect((msg["ip"],int(msg["port"])))
                        client_socket.send(msg.encode())
                        client_socket.close()
                    else:raise ValueError
                else:raise ValueError
            else:raise ValueError
        except ValueError as e:
            message={
                "type":"reply",
                "leader_id":self.server_id,
                "ip":self.ip,
                "port":self.port,
                "commit_status":False,
                "mid":msg["mid"]
                }
            message=json.dumps(message)
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((msg["ip"],int(msg["port"])))
            client_socket.send(msg.encode())
            client_socket.close()
        

    def sendCommit(self,msg):
        # if (committed from majority):
            # message = Message({"""Refer to the format of normal-committed"""})
            # for client in self.cluster:
            #     message.sendTo(client)
        return 
    # def update(self):
        # self.storage.update()
    def valid_commit(self,msg):
        return
    def committed(self,msg):
        return