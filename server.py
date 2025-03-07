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
        self.crashed = False
        self.cluster_partitioned = False
        self.partitioned_with = []
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
        self.timeout_limit=random.uniform(1, 2)
        self.last_message_time=time.time()
        self.leader_exist=0
        self.pending_elec={}
        self.elec_id=None
        self.msg_q=[]
        self.currrent_vote_term=0

        self.cur_term = 0
        self.commit_index = [0, 0]
        self.cur_index = [0, 0]
        self.logList = []
        self.commited_logList = []
        self.initLog()

        threading.Thread(target = lambda : self.as_leader_hb()).start()
        threading.Thread(target = lambda : self.monitor_timeout()).start()
        threading.Thread(target = lambda : self.listening()).start()

        
        #------------ 以上已完成初始化，以下是需要实现的 ------------
        self.match_index = [0, 0]  # Already replicated on server
        # self.log_temp = None # 临时存储log

# "1":{"x":"s1","y":"s2","amt":5},"2":{"x":"s4","y":"s5","amt":1}}, "data": {"1": {"customer_id": 1, "balance": 10}, "2": {"customer_id": 2, "balance": 10}}}
    def initLog(self):
        self.logList = self.log_db.all()
        self.commited_logList = self.log_db.all().copy()
        if len(self.logList) == 0:
            return
        else:
            self.cur_term = self.logList[len(self.logList)-1]["term"]
            self.commit_index[0] = self.cur_term
            self.commit_index[1] = len(self.logList)
            self.cur_index = self.commit_index.copy()
            print(f"[{self.server_id}] init index: {self.cur_index}, init commit_index: {self.commit_index}")
        
    def as_leader_hb(self):
        while True:
            while not self.crashed:
                time.sleep(0.5)
                if self.leader==self.server_id:
                    #print(f"[{self.server_id}] send heartbeat")
                    if len(self.msg_q)==0:
                        message={
                            "type":"hb",
                            "leader_id":self.server_id,
                            "ip":self.ip,
                            "port":self.port,
                            "cur_index":self.cur_index,
                            "commit_index":self.commit_index,
                            # "commited_log_list":self.commited_logList,
                            # "log_list":self.logList
                        }
                        if len(self.logList) != 0:
                            message["newest_log_type"]= self.logList[len(self.logList)-1]["transaction_type"]
                        message.update(self.server_info)
                        message=json.dumps(message)
                        for other in self.others:
                            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                            my_socket.connect((other["ip"],int(other["port"])))
                            my_socket.send(message.encode())
                            my_socket.close()
                    else:
                        for index in range(len(self.msg_q)):
                            msg=self.msg_q[index]
                            msg=json.dumps(msg)
                            for other in self.others:
                                my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                my_socket.connect((other["ip"],int(other["port"])))
                                # print(f"[{self.server_id}] send msg to {other['server_id']}")
                                # print(other["ip"],int(other["port"]))
                                my_socket.send(msg.encode())
                                my_socket.close()
                                time.sleep(0.1)
                        self.msg_q=[]
                    self.last_message_time=time.time()
                else:
                    continue
                
    def listening(self):
        while True:
            client_socket, address = self.socket.accept()
            received_data = b''
            while True:
                chunk = client_socket.recv(8192)  # Increased buffer size
                if not chunk:
                    break
                else:
                    received_data += chunk
                    data = received_data.decode('utf-8')
                    # print(f"[{self.server_id}] listened")
                    msg_data = json.loads(data)
                    if "server_id" in msg_data and msg_data["server_id"] == self.leader:
                        self.last_message_time=time.time()
                    if self.crashed:
                        if msg_data["type"] == "recover":
                            self.crashed = False
                            print(f"[{self.server_id}] recover!")
                        else:
                            continue
                    # if "server_id" in msg_data and msg_data["server_id"] == 'S2':
                    #     print(f"[{self.server_id}] receive msg from {msg_data['server_id']}, {msg_data['type']}")
                    match msg_data["type"]:
                        case "crash":
                            print(f"[{self.server_id}] crash!")
                            self.crashed = True
                        case "hb":
                            # print(f"[{self.server_id}] receive hb")
                            self.checkHb(msg_data)
                        case "you_are_not_leader":
                            self.stepDown(msg_data)
                        case "elec":
                            # print(f"[{self.server_id}] receive elec,need to vote!")
                            self.vote(msg_data)
                        case "vote":
                            # print(f"[{self.server_id}] receive vote!")
                            self.handle_vote(msg_data)
                        case "init_Raft":
                            print(f"[{self.server_id}] receive in-cluster client transaction request, handling!")
                            if self.cluster_partitioned != True:
                                self.request(msg_data,"raft")
                        case "init_2PC":
                            print(f"[{self.server_id}] receive x-cluster client transaction request, handling!")
                            if self.cluster_partitioned != True:
                                self.request(msg_data,"2pc")
                        case "request":
                            # print(f"[{self.server_id}] receive commit request")
                            self.reply(msg_data)
                        case "reply":
                            print(f"[{self.server_id}] receive reply")
                            self.sendCommit(msg_data)
                        case "commit":
                            print(f"[{self.server_id}] receive committed")
                            self.committed(msg_data)
                        case "need_log":
                            print(f"[{self.server_id}] receive need_log")
                            self.sendLog(msg_data)
                        case "log":
                            print(f"[{self.server_id}] receive log")
                            self.updateLog(msg_data)
                        case "2pc_commit":
                            print(f"[{self.server_id}] receive 2pc_commit")
                            self.twoPcCommitted(msg_data)
                        case "2pc_abort":
                            print(f"[{self.server_id}] receive 2pc_abort")
                            self.twoPcAbort(msg_data)
                        case "partition":
                            print(f"[{self.server_id}] partitioned!")
                            self.crashed = True
                        case "partition_cluster":
                            print(f"[{self.server_id}] cluster partitioned!")
                            self.cluster_partitioned = True
                        case "recover_cluster":
                            print(f"[{self.server_id}] cluster recovered!")
                            self.cluster_partitioned = False
                
                        
            
                    # if (msg_data["code"] == "check_rply"):
                    #     print("check_rply")
                    # if (msg_data["code"] == "release"):
                    #     print("release")


    def initMyStorage(self, db_file_name):
        # print("initMyStorage")
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
            while not self.crashed:
                time.sleep(0.5)
                if (self.leader!=self.server_id):
                    time_gap=time.time()-self.last_message_time
                    if time_gap>self.timeout_limit:
                        print(f"[{self.server_id}] long time no receive msgs,timeout!")
                        self.leader_exist=0
                        self.leader=None
                        self.election()

    def vote(self,msg):
        #compare index term 
        this_vote = True
        if self.currrent_vote_term < msg["last_included_term"]:
            self.currrent_vote_term = msg["last_included_term"]
        else:
            this_vote = False
        
        if msg["last_included_term"]>self.cur_term and this_vote == True:
            self.last_message_time=time.time()
            self.cur_term=msg["last_included_term"]
            self.cur_index[0]=msg["last_included_term"]
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
        elif msg["last_included_term"]==self.cur_term and msg["last_included_index"]>self.cur_index and this_vote == True: 
            self.last_message_time=time.time()
            self.cur_term=msg["last_included_term"]
            self.cur_index[0]=msg["last_included_term"]
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
            my_socket.close()
        else:
            self.currrent_vote_term -= 1
            message={
                "type":"vote",
                "voter_id":self.server_id,
                "term":self.cur_term,
                "reply_to_mid":msg["mid"],
                "granted_vote":False,
                "leader_id":self.leader
            }
            message.update(self.server_info)
            message=json.dumps(message)
            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            my_socket.connect((msg["ip"],int(msg["port"])))
            my_socket.send(message.encode())
            print(f"[{self.server_id}] rejected {msg["candidate_id"]}'s election with my term:{self.cur_term}!")
            my_socket.close()
            if(self.leader==None and self.leader_exist==0):
                self.election()

    def handle_vote(self,msg):
        if msg["reply_to_mid"]==self.elec_id and self.pending_elec!={}:
            #print("111")
            if msg["granted_vote"]==True:
                self.pending_elec[self.elec_id]["others_replied"].append(True)
                if len(self.pending_elec[self.elec_id]["others_replied"]) > 1:
                    self.leader=self.server_id
                    self.leader_exist=1
                    #广播给client
                    message={
                        "type":"leader",
                        "leader_port":self.port,
                    }
                    message.update(self.server_info)
                    message=json.dumps(message)
                    my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    my_socket.connect(("127.0.0.1",5010))
                    my_socket.send(message.encode())
                    my_socket.close()

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
                self.leader=msg["leader_id"]
                if self.leader!=None:
                    self.leader_exist=1
                self.pending_elec={}
                print("vote fail")
                return
        else:
            return
    
    # message={
    #     "type":"hb",
    #     "leader_id":self.server_id,
    #     "ip":self.ip,
    #     "port":self.port,
    #     "index":self.cur_index
    # }
    # message.update(self.server_info)

    def checkHb(self,msg):
        checkFail = False
        if msg["cur_index"][0] < self.cur_term:
            checkFail= True
        if msg["cur_index"][0] == self.cur_term and msg["cur_index"][1] < self.cur_index[1]:
            checkFail= True
            if "newest_log_type" in msg and msg["cur_index"][1] + 1 == self.cur_index[1] and msg["newest_log_type"] == "2pc":
                checkFail = False
        if checkFail== True:
            print(f"[{self.server_id}] you are not leader, you step down!")
            massage = {
                "type":"you_are_not_leader",
                "leader_id":self.leader,
                "ip":self.ip,
                "port":self.port
            }
            massage.update(self.server_info)
            massage=json.dumps(massage)
            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            my_socket.connect((msg["ip"],int(msg["port"])))
            my_socket.send(massage.encode())
            my_socket.close()
        else:
            self.checkLogConsistency(msg)
            return


# message={
#     "type":"hb",
#     "leader_id":self.server_id,
#     "ip":self.ip,
#     "port":self.port,
#     "cur_index":self.cur_index,
#     "commit_index":self.commit_index,
#     "commited_log_list":self.commited_logList,
#     "log_list":self.logList
# }

    def checkLogConsistency(self,msg):
        # print(f"[{self.server_id}] check log consistency")
        # if self.logList != msg["log_list"]:
        #     print(f"[{self.server_id}] logList is not consistent")
        if self.cur_index != msg["cur_index"]:
            # print(f"[{self.server_id}] cur_index is not consistent")
            # print(f"[{self.server_id}] my cur_index: {self.cur_index}, his cur_index: {msg['cur_index']}")
            massage = {
                "type":"need_log",
                "follower_id":self.server_id,
                "ip":self.ip,
                "port":self.port
            }
            massage.update(self.server_info)
            massage=json.dumps(massage)
            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            my_socket.connect((msg["ip"],int(msg["port"])))
            my_socket.send(massage.encode())
            my_socket.close()
            print(f"[{self.server_id}] in checkLogConsistency my index {self.cur_index}")

    def sendLog(self,msg):
        message={
            "type":"log",
            "leader_id":self.server_id,
            "ip":self.ip,
            "port":self.port,
            "cur_index":self.cur_index,
            "commit_index":self.commit_index,
            "commited_log_list":self.commited_logList,
            "log_list":self.logList
        }
        message.update(self.server_info)
        message=json.dumps(message)
        my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        my_socket.connect((msg["ip"],int(msg["port"])))
        my_socket.send(message.encode())
        my_socket.close()
        print(f"[{self.server_id}] in sendLog my index {self.cur_index}")


    def updateLog(self,msg):
        self.cur_index = msg["cur_index"].copy()
        self.logList = msg["log_list"]
        # print(f"[{self.server_id}] updated log: {self.logList}, index: {self.cur_index}")
        commited_logs_to_be_done = []
        if len(msg["commited_log_list"]) == 0:
            print(f"[{self.server_id}] no commited logs")
            return
        # for i in range(msg["commit_index"][1]+1):
        #     if self.commited_logList[i] != msg["commited_log_list"][i]:
        #         commited_logs_to_be_done.append(i)
        i = len(self.commited_logList)
        # print(f"[{self.server_id}] commited logs: {self.commited_logList}")
        while i < len(msg["commited_log_list"]):
            commited_logs_to_be_done.append(msg["commited_log_list"][i])
            i += 1
        print(f"[{self.server_id}] in updateLog my index: {self.cur_index}")
        for b in commited_logs_to_be_done:
            self.excuteLog(b)        
        return
    
    def excuteLog(self,log):
        self.commit_index[0]=log["term"]
        self.commit_index[1]+=1
        log["index"] = self.commit_index.copy()
        self.commited_logList.append(log)
        query = Query()
        if log["transaction_type"]=="raft":
            x_balance = self.data_db.search(query.customer_id == log['x'])[0]['balance'] - log['amt']
            y_balance = self.data_db.search(query.customer_id == log['y'])[0]['balance'] + log['amt']
            self.data_db.update({'balance': x_balance}, query.customer_id == log['x'])
            self.data_db.update({'balance': y_balance}, query.customer_id == log['y'])
        else:
            if self.cluster == 1:
                this_range = range(1,1000)
            elif self.cluster == 2:
                this_range = range(1001,2000)
            elif self.cluster == 3:
                this_range = range(2001,3000)
            if log['x'] in this_range:
                x_balance = self.data_db.search(query.customer_id == log['x'])[0]['balance'] - log['amt']
                self.data_db.update({'balance': x_balance}, query.customer_id == log['x'])
            elif log['y'] in this_range:
                y_balance = self.data_db.search(query.customer_id == log['y'])[0]['balance'] + log['amt']
                self.data_db.update({'balance': y_balance}, query.customer_id == log['y'])
        self.log_db.insert(log)
        print(f"[{self.server_id}] in excute index: {self.cur_index}")
        # print(f"[{self.server_id}] commited_logList: {self.commited_logList[len(self.commited_logList)-1]}, index: {self.commit_index}")
        
        # print(f"[{self.server_id}] excute log: {log}")
        # print(self.commited_logList)
        # print(self.commit_index)
        return

    def stepDown(self, msg):
        # print(f"[{self.server_id}] I step down because of {msg["server_id"]}!")
        self.leader = msg["leader_id"]
        if self.leader != None:
            self.leader_exist = 1
        print(f"[{self.server_id}] OK I step down because of {msg["server_id"]}!")
        
        
    def request(self,msg,transaction_type):
        # print(msg)
        x=msg["transaction_sourse"]
        y=msg["transaction_target"]
        amt=msg["transaction_amount"]
        if self.cluster == 1:
            this_range = range(1,1001)
        elif self.cluster == 2:
            this_range = range(1001,2001)
        elif self.cluster == 3:
            this_range = range(2001,3001)
        if x in this_range:
            query = Query()
            x_data = self.data_db.search(query.customer_id == x)[0]
            # print(x_data)
            new_x_ballance = x_data["balance"]-amt
        else:
            new_x_ballance = 1
        if new_x_ballance >= 0:
            self.cur_index[0]=self.cur_term
            self.cur_index[1]+=1
            print(f"[{self.server_id}] in request my index to {self.cur_index}")
            # print(f"[{self.server_id}] I incrementaled my index to {self.cur_index}")
            this_log={}
            this_log={
                "term":self.cur_term,
                "x":x,
                "y":y,
                "amt":amt,
                "commit_status":False,
                "vote_num":1,
                "mid":msg["mid"],
                "transaction_type":transaction_type,
                "2pc_confirm_sent":False

            }
            this_log["index"] = [self.cur_index[0],len(self.logList)+1]
            
            self.logList.append(this_log)
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
                "mid":msg["mid"],
                "transaction_type":transaction_type,
                "logList":self.logList
                }
            message.update(self.server_info)
            self.msg_q.append(message)
            # print(f"[{self.server_id}] in request log: {self.logList}, index: {self.cur_index}")
        elif transaction_type == "raft":
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
            print(f"[{self.server_id}] abort transaction")
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(("127.0.0.1",5010))
            client_socket.send(message.encode())
            client_socket.close()
            return
        elif transaction_type == "2pc":
            message={
                "type":"2pc_abort_from_server",
                "leader_id":self.server_id,
                "ip":self.ip,
                "port":self.port,
                "x":x,
                "y":y,
                "amt":amt,
                "mid":msg["mid"],
                "abort_server":self.port
                }
            message.update(self.server_info)
            message=json.dumps(message)
            # print(f"[{self.server_id}] abort 2pc transaction")
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(("127.0.0.1",5010))
            client_socket.send(message.encode())
            client_socket.close()
        return
            #发回给router，中断

# message={
#                 "type":"request",
#                 "leader_id":self.server_id,
#                 "last_included_index":self.commit_index,
#                 "last_included_term":self.cur_term,
#                 "cur_index":self.cur_index,
#                 "ip":self.ip,
#                 "port":self.port,
#                 "x":x,
#                 "y":y,
#                 "amt":amt,
#                 "mid":msg["mid"],
#                 "transaction_type":transaction_type
#                 }

    def reply(self,msg):
        # print(f"[{self.server_id}] receive commit request from {msg["server_id"]}")
        x=msg["x"]
        y=msg["y"]
        amt=msg["amt"]
        try:
            if msg["cur_index"][0]>=self.cur_term and msg["cur_index"][1]>=self.cur_index[1]:
                if self.cluster == 1:
                    this_range = range(1,1001)
                elif self.cluster == 2:
                    this_range = range(1001,2001)
                elif self.cluster == 3:
                    this_range = range(2001,3001)
                if x in this_range:
                    query = Query()
                    x_data = self.data_db.search(query.customer_id == x)[0]
                    # print(x_data)
                    new_x_ballance = x_data["balance"]-amt
                else:
                    new_x_ballance = 1
                if new_x_ballance >= 0:
                    #最后判断余额
                    # self.cur_index=msg["cur_index"]
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
                    client_socket.send(message.encode())
                    client_socket.close()
                    print(f"[{self.server_id}] in rply my index to {self.cur_index}")
                else:raise ValueError
                # else:raise ValueError
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
            client_socket.send(message.encode())
            client_socket.close()
        # print(f"[{self.server_id}] in reply log: {self.logList}, index: {self.cur_index}")
        
# this_log={
#     "term":self.cur_term,
#     "index":self.cur_index,
#     "x":x,
#     "y":y,
#     "amt":amt,
#     "commit_status":False,
#     "vote_num":1,
#     "mid":msg["mid"],
#     "transaction_type":transaction_type
# }
    def sendCommit(self,msg):
        # if (committed from majority):
            # message = Message({"""Refer to the format of normal-committed"""})
            # for client in self.cluster:
            #     message.sendTo(client)
        for log in self.logList:
            if log["mid"]==msg["mid"]:
                log["vote_num"]+=1
        for log in self.logList:
            if log["vote_num"]>=2 and log["commit_status"]==False and log["transaction_type"]=="raft":
                log["commit_status"]=True
                message={
                    "type":"commit",
                    "leader_id":self.server_id,
                    "ip":self.ip,
                    "port":self.port,
                    "mid":msg["mid"]
                }
                message.update(self.server_info)
                message=json.dumps(message)
                print(f"[{self.server_id}] in sendCommit my index to {self.cur_index}")
                self.excuteLog(log)
                for other in self.others:
                    my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    my_socket.connect((other["ip"],int(other["port"])))
                    my_socket.send(message.encode())
                    my_socket.close()
                    # print(f"[{self.server_id}] in commit log: {self.logList}, index: {self.cur_index}")
                message2={
                    "type":"this_raft_commited",
                    "leader_id":self.server_id,
                    "ip":self.ip,
                    "port":self.port,
                    "mid":msg["mid"]
                }
                print(f"[{self.server_id}] in sendCommit my index to {self.cur_index}")
                message2=json.dumps(message2)
                my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                my_socket.connect(("127.0.0.1",5010))
                my_socket.send(message2.encode())
                my_socket.close()
                
            if log["vote_num"]>=2 and log["commit_status"]==False and log["2pc_confirm_sent"] == False and log["transaction_type"]=="2pc":
                log["2pc_confirm_sent"] = True
                message={
                    "type":"2pc_can_commit",
                    "leader_id":self.server_id,
                    "ip":self.ip,
                    "port":self.port,
                    "mid":msg["mid"]
                }
                if self.cluster == 1:
                    this_range = range(1,1001)
                elif self.cluster == 2:
                    this_range = range(1001,2001)
                elif self.cluster == 3:
                    this_range = range(2001,3001)
                if log["x"] in this_range:
                    message["transaction_sourse_can"] = 1
                    message["transaction_target_can"] = 0
                if log["y"] in this_range:
                    message["transaction_sourse_can"] = 0
                    message["transaction_target_can"] = 1
                message.update(self.server_info)
                message=json.dumps(message)
                print(f"[{self.server_id}] in sendCommit my index to {self.cur_index}")
                my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                my_socket.connect(("127.0.0.1",5010))
                my_socket.send(message.encode())
                my_socket.close()
                # print(f"[{self.server_id}] in commit log: {self.logList}, index: {self.cur_index}")
                return
        return 
    def twoPcCommitted(self,msg):
        for log in self.logList:
            if log["mid"]==msg["mid"]:
                log["commit_status"]=True
                self.excuteLog(log)
                print(f"[{self.server_id}] in twoPcCommitted my index to {self.cur_index}")
                return
        return
    def twoPcAbort(self,msg):
        # for log in self.logList:
        #     if log["mid"]==msg["mid"]:
        #         self.logList.remove(log)
        #         self.cur_index[1]-=1
        #         print(f"[{self.server_id}] they ask me to abort msg {msg['mid']}")
        # for i in range(0,len(self.logList)):
        #     self.logList[i]["index"][1]=i
        return


    def committed(self,msg):
        return