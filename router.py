import socket
import threading
import json

class Router:
    def __init__(self):
        self.leader_list={
            "cluster1":"",
            "cluster2":"",
            "cluster3":""
            }  
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.bind(("127.0.0.1",5011))
        self.client_socket.listen(30)
        self.partitions = {"cluster1": False, "cluster2": False, "cluster3": False}
        self.crashed = False
        self.cluster_connections = {}
        threading.Thread(target=self.listening, daemon=True).start()
        print("Router started")

    def listening(self):
        while True:
            try:
                client_socket, address = self.client_socket.accept()
                if self.crashed:
                    client_socket.close()
                    continue
                threading.Thread(target=self.handle_client, args=(client_socket,), daemon=True).start()
            except Exception as e:
                if not self.crashed:
                    print(f"Error accepting connection: {e}")
    
    def handle_client(self, client_socket):
        try:
            while True:
                data = client_socket.recv(1024).decode()
                if not data:
                    break
                msg_data = json.loads(data)
                if self.crashed and msg_data["code"] != "recover":
                    response = {"status": "error", "message": "Router is crashed"}
                    client_socket.send(json.dumps(response).encode())
                    continue
                if msg_data["code"] == "Leader":
                    self.leader_list[msg_data["cluster"]] = msg_data["leader_port"]
                    response = {"status": "success", "message": f"Leader updated for {msg_data['cluster']}"}
                elif msg_data["code"] == "start_2PC":
                    response = {"status": "success", "message": "2PC initiated"}
                elif msg_data["code"] == "start_raft":
                    response = {"status": "success", "message": "Raft consensus initiated"}
                elif msg_data["code"] == "crash":
                    self.crash()
                    response = {"status": "success", "message": "Router crashed"}
                elif msg_data["code"] == "recover":
                    self.recover()
                    response = {"status": "success", "message": "Router recovered"}
                elif msg_data["code"] == "print_balance":
                    balance_info = self.print_balance()
                    response = {"status": "success", "balance": balance_info}
                    balance_info = self.print_balance()
                    response = {"status": "success", "balance": balance_info}
                elif msg_data["code"] == "partition":
                    self.partition(msg_data["cluster"])
                    response = {"status": "success", "message": f"Cluster {msg_data['cluster']} partitioned"}
                elif msg_data["code"] == "partition_cluster":
                    self.partition_cluster(msg_data["cluster"])
                    response = {"status": "success", "message": f"Cluster {msg_data['cluster']} partitioned"}
                elif msg_data["code"] == "recover_cluster":
                    self.recover_cluster(msg_data["cluster"])
                    response = {"status": "success", "message": f"Cluster {msg_data['cluster']} recovered"}
                else:
                    response = {"status": "error", "message": "Unknown command"}
                client_socket.send(json.dumps(response).encode())
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def crash(self):
        self.crashed = True
        print("Router crashed")
        for conn in list(self.cluster_connections.values()):
            try:
                conn.close()
            except:
                pass
        self.cluster_connections.clear()

    def recover(self):
        self.crashed = False
        print("Router recovered")
        for cluster in self.partitions:
            self.partitions[cluster] = False

    def print_balance(self):
        balance_info = {}
        for cluster, leader_port in self.leader_list.items():
            if not leader_port or self.partitions[cluster]:
                balance_info[cluster] = "Unavailable"
                continue
                
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(("127.0.0.1", int(leader_port)))
                request = json.dumps({"code": "get_balance"})
                s.send(request.encode())
                
                response = s.recv(1024).decode()
                balance_data = json.loads(response)
                balance_info[cluster] = balance_data
                s.close()
            except Exception as e:
                balance_info[cluster] = f"Error: {str(e)}"
                
        print("Balance information:", balance_info)
        return balance_info

    def partition(self, cluster):
        self.partitions[cluster] = True
        print(f"Cluster {cluster} partitioned")
        if cluster in self.cluster_connections:
            try:
                self.cluster_connections[cluster].close()
            except:
                pass
            del self.cluster_connections[cluster]

    def partition_cluster(self, cluster):
        clusters = cluster.split(',')
        for c in clusters:
            if c.strip() in self.partitions:
                self.partition(c.strip())

    def recover_cluster(self, cluster):
        self.partitions[cluster] = False
        print(f"Cluster {cluster} recovered")
        if cluster in self.leader_list and self.leader_list[cluster]:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(("127.0.0.1", int(self.leader_list[cluster])))
                self.cluster_connections[cluster] = s
            except:
                pass
