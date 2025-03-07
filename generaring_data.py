import json
from tinydb import TinyDB
import os

def create_server_database(server_id, cluster, db_file):
    if os.path.exists(db_file):
        os.remove(db_file)
    db = TinyDB(db_file)
    logs_table = db.table('logs')   
    data_table = db.table('data')
    if cluster == 1:
        start_id, end_id = 1, 1000
    elif cluster == 2:
        start_id, end_id = 1001, 2000
    elif cluster == 3:
        start_id, end_id = 2001, 3000
    else:
        raise ValueError("Cluster num invalid.")
    customers = [{"customer_id": cid, "balance": 10} for cid in range(start_id, end_id + 1)]
    logs=[]
    logs_table.insert_multiple(logs)
    data_table.insert_multiple(customers)
    
    db.close()

def main():
    servers_info = []
    for i in range(1, 10):
        server_id = f"S{i}"
        if i <= 3:
            cluster = 1
        elif i <= 6:
            cluster = 2
        else:
            cluster = 3

        ip_address = "127.0.0.1"
        port = f"500{i}"

        db_file = f"data/db_server_{server_id}.json"
        create_server_database(server_id, cluster, db_file)
        servers_info.append({
            "server_id": server_id,
            "cluster": cluster,
            "ip": ip_address,
            "port": port,
            "db_file": db_file
        })
    with open("data/servers_info.json", "w") as f:
        json.dump(servers_info, f, indent=4)
    
    print("Initialization complete: 9 server database files and servers_info.json file created.")

if __name__ == "__main__":
    main()