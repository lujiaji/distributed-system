
class Message:
    def __init__(self,msg):
        self.target=None
        self.msg=msg
        """
        Could be found in "messages_example.json"
        {
            "type":"elec",
            "tran":{
                "term",
                "candidated_id":self.id,
                "last_included_index",#needed when elec
                "last_included_term",#needed when elec
            },
            "mid":1,
        },
        {
            "type":"vote",
            "tran":{
                "term":cur_term
                },
            "mid":1,
            "granted_vote":True/False
        },
        {
            "type":"normal",
            "tran":{
                "phase":"request",
                "term",
                "leader_id",
                "last_included_index",
                "last_included_term",
                "entries":[
                    {"x","y","amt"},
                    {"a","b","amt2"}
                    ], #could be a lot
                "commit_result":None
            },
            "mid":2,
        },
        {
            "type":"normal",
            "tran":{
                "phase":"reply_request",
                "term",
                "leader_id",
                "last_included_entry":{{entries},[term,index]},
                "commit_result":True
            },
            "mid":2,
        },
        {
            "type":"normal",
            "tran":{
                "phase":"commited",
                "term",
                "leader_id",
                "last_included_entry":{{entries},[term,index]},
            },
            "mid":2,
        },
        {
            "type":"C_change",
            "mid":3,
            "C_add"=[new_servers]
        }
        """
    def sendTo(self,target):
        #do send, with dict message
        return 0

class BalanceTable:
    def __init___(self):
        self.clients=[]
        self.balance_table=[{"":0},
                            {"":0}]
class Storage:
    def __init__(self):
        self.storage=None
        self.balance_table=BalanceTable()
        #make a dict to store balance and transactions
    def validLog(self,tran):
        return  tran==self.storage
    def validBalance(self,client,amt):
        if client in self.clients:
            if (self.balance_table[client]>=amt):
                return True
            else: return False
        else:
            return False
    def valid(self,tran,client,amt):
        return (self.validLog(tran) and self.validBalance(client,amt))
    
    def changeLog(self):
        return 0
    def changeBalance(self,client,amt):
        if client in self.clients:
            self.balance_table[client]+=amt
            return True
        else:
            return False
    
    def update(self,client,amt):
        self.changeLog()
        self.changeBalance(client,amt)


class Server:
    def __init__(self):
        #initial some param
        self.id=None#read from json
        self.storge=Storage()
        self.mode=None # "election" "normal" "C_change"
        self.cluster=None# need to init

        #persistent state,回复RPC前要在Storage里面更新的
        self.cur_term=0#monotonically increase, or sync with current leader
        self.leader=None#To vote and remember current leader
        self.log=None#需要初始化

        #Volatile State on all servers
        self.commit_index=[0,0]#committed entry index
        
        #Volatile State on leaders,每次选举后要清零（特指index）
        self.cur_index=[0,0]#[term,index]
        self.match_index=[0,0]#already replicated on server

        #
    """按照执行顺序，对函数进行串联"""
    def election(self,target):
        message=Message({"""参照elec的格式"""})
        for client in self.cluster:
            message.sendTo(client)
    #def receiveMsgs(self):
    """分为三种情况,"election" "normal" "C_change",
    可以通过msg的dict的最外层key区分
    if(最外层是"normal"):
        switch("phase"):
            case "request":
                do sendReply()
            case "reply_request":
                do sendCommmit()
            case "commited":
                do self.update()
    """
    #def receiveClientRequest(self):

    #def sendRequest(self):

    def sendReply(self):
        if (self.storge.valid()):#fill it
            message=Message({"""参照normal-replyRequest的格式"""})
            message.sendTo(self.leader)

    #def sendCommit(self):
        # if (committed from majority):
            # message=Message({"""参照normal-committed的格式"""})
            # for client in self.cluster:
            #     message.sendTo(client)

    #def update(self):
        #self.storge.update()