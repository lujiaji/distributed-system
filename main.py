import threading
from multiprocessing import Process
import time
import json
import socket
import hashlib
import tkinter
import random
import sys
import server
import client
from tinydb import TinyDB, Query

def start_server(server_info,servers):
    return server.Server(server_info,servers)

def PrintBalance(client_id):
    numeric_str = ''.join(filter(str.isdigit, client_id))
    if numeric_str:
        client_id = int(numeric_str)
    else:
        print("id wrong")
    if 1 <=client_id<= 1000:
        bias=1
    elif 1001 <=client_id<= 2000:
        bias=4
    elif 2001 <=client_id<= 3000:
        bias=7
    else:
        print("id wrong")
        return
    for i in range(0,3):
        db=TinyDB(f'data/db_server_S{bias+i}.json')
        balance_table=db.table('data')
        user=Query()
        result=balance_table.search(user.customer_id==int(client_id))
        if result:
            print(f"{client_id} has balance: {result[0]['balance']}")
        else:
            print(f"error")
def PrintDatastore():
    for i in range(1,10):
        db=TinyDB(f'data/db_server_S{i}.json')
        log_table=db.table('logs')
        print(f"{i}th server recorded logs:")
        for log in log_table.all():
            print(f"index: {log["index"]}opration:({log["x"]} {log["y"]} {log["amt"]})")

if __name__ == "__main__":
    print("welcome to the bank system!")
    print("-----Author: Zhaohongzhi Cai, Jiaji Lu-----")
    print("copyright reserved 2025")
    print("command options: print_balance, print_datastore, performance, exit")
    config_file_path = "data/servers_info.json"
    with open(config_file_path, "r") as file:
        servers = json.load(file)
    for server_info in servers:
        Process(target=start_server, args=(server_info,servers)).start()
    client_server = client.Client(servers)
    while True:
        command = input("").strip().lower()
        command = command.split(" ")
        if command[0] == "print_balance":
            client_id = input("ClientID: ").strip()
            PrintBalance(client_id)
        elif command[0] == "print_datastore":
            PrintDatastore()
        elif command[0] == "performance":
            print(client_server.messageTime)
        elif command[0] == "exit":
            print("exit system!")
            break
        elif command[0] == "crash":
            client_id = input("ClientID: ").strip()
            client_server.crashServer(client_id)
        elif command[0] == "recover":
            client_id = input("ClientID: ").strip()
            client_server.recoverServer(client_id)
        elif command[0] == "partition":
            client_id = input("ClientID: ").strip()
            client_server.partitionServer(client_id)
        elif command[0] == "partition_cluster":
            client_id = input("cluster: ").strip()
            client_server.partitionCluster(client_id)
        elif command[0] == "recover_cluster":
            client_id = input("cluster: ").strip()
            client_server.recoverCluster(client_id)
        elif command[0].isdigit() and int(command[0]) in range(1, 3001):
            client_server.initTransactionMessage(command)
            print("ok WE GOT A NUMBER")
        else:
            print("unknown command, please try again.")
