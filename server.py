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
        print(f"{self.server_id}:{self.others}")

        self.id = None  # Read from JSON
        self.data_base = self.initMyStorage(self.db_file)
        self.data_db = self.data_base.table('data')
        self.log_db = self.data_base.table('logs')  # Needs to be initialized

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip,int(self.port)))
        self.socket.listen(30)


        threading.Thread(target = lambda : self.listening()).start()
        threading.Thread(target = lambda : self.monitor_timeout()).start()

        
        #------------ 以上已完成初始化，以下是需要实现的 ------------

        self.mode = None  # "election", "normal", "C_change"
        # Persistent state, update in Storage before replying to RPC
        self.cur_term = 0  # Monotonically increase, or sync with current leader
        self.leader = None  # To vote and remember current leader
        # Volatile state on all servers
        self.commit_index = [0, 0]  # Committed entry index
        # Volatile state on leaders, reset after each election (specifically index)
        self.cur_index = [0, 0]  # [term, index]
        self.match_index = [0, 0]  # Already replicated on server
        self.log_temp = None # 临时存储log
        self.timeout_limit=1
        self.last_message_time=time.time()
        self.leader_exist=0

    def listening(self):
        while True:
            print(f"[{self.server_id}] listened")
            client_socket, address = self.socket.accept()
            while True:
                data = client_socket.recv(1024).decode()
                # add delay here
                # mySleep(self.sleepType)
                if not data:
                    break
                else:
                    self.last_message_time=time.time()
                    msg_data = json.loads(data)
                    if (msg_data["type"] == "elec"):
                        print(f"[{self.server_id}] receive elec,need to vote!")
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
        message={
            "type":"elec",
            "tran":{
                "term":self.cur_term,
                "candidated_id":self.server_id,
                "last_included_index":self.commit_index,
                "last_included_term":self.cur_term+1
                },
            "mid":hashlib.md5(str(random.random()).encode()).hexdigest(),
            }
        message=json.dumps(message)
        for other in self.others:
            my_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            my_socket.connect((other["ip"],int(other["port"])))
            my_socket.send(message.encode())
            my_socket.close()

        # for client in self.cluster:
        #     message.sendTo(client)

        return

    def monitor_timeout(self):
        while True:
            time.sleep(0.5)
            time_gap=time.time()-self.last_message_time
            if time_gap>self.timeout_limit:
                print(f"[{self.server_id}] long time no receive msgs,timeout!")
                self.leader_exist=0
                self.leader=None
                self.election()

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
