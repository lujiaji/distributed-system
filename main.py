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

def start_server(server_info):
    server.Server(server_info)

if __name__ == "__main__":

    # Initialize the environment, such as loading the database, initializing servers/clients, etc.
    # initialize_environment()  # Pseudo function, represents your initialization code

    print("welcome to the bank system!")
    print("command options: print_balance, print_datastore, performance, exit")
    config_file_path = "data/servers_info.json"
    with open(config_file_path, "r") as file:
        servers = json.load(file)
    for server_info in servers:
        print(server_info["db_file"])
        Process(target=start_server, args=(server_info,)).start()
    client_server = client.Client()
    while True:
        # Read input from the command line
        command = input("").strip().lower()
        command = command.split(" ")
        if command[0] == "print_balance":
            # Pseudo code: Get client ID, then call the function to print balance
            client_id = input("ClientID: ").strip()
            # PrintBalance(client_id)  # Please replace with your actual function
        elif command[0] == "print_datastore":
            print("ok")
            # PrintDatastore()  # Please replace with your actual function
        elif command[0] == "performance":
            print("ok")
            # Performance()  # Please replace with your actual function
        elif command[0] == "exit":
            print("exit system!")
            break
        elif int(command[0]) in range(1, 3001):
            client_server.initTransactionMessage(command)
            print("ok WE GOT A NUMBER")
        else:
            print("unknown command, please try again.")
