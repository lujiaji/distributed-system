import json
from tinydb import TinyDB
import os
# this file is used to generate data for the bank system
# the data is stored in the data folder
# the data is stored in the json format
def create_server_database(server_id, cluster, db_file):
    """
    Create a database file for the specified server and initialize two tables:
    - logs: store log records (initially empty)
    - data: store customer data (1000 data items, each with an initial balance of 10)
    
    Different clusters store different ranges of customer data:
      - Cluster 1: customer_id 1 ~ 1000
      - Cluster 2: customer_id 1001 ~ 2000
      - Cluster 3: customer_id 2001 ~ 3000
    """
    # If the file already exists, overwrite it (you can also delete the old file first)
    if os.path.exists(db_file):
        os.remove(db_file)
    
    # Open the database file (create if it does not exist)
    db = TinyDB(db_file)
    
    # get the tables：logs 和 data
    logs_table = db.table('logs')   
    data_table = db.table('data')
    
    # 根据所属集群确定数据范围
    if cluster == 1:
        start_id, end_id = 1, 1000
    elif cluster == 2:
        start_id, end_id = 1001, 2000
    elif cluster == 3:
        start_id, end_id = 2001, 3000
    else:
        raise ValueError("Cluster num invalid.")
    
    # Prepare customer data records, each containing customer_id and initial balance (10)
    customers = [{"customer_id": cid, "balance": 10} for cid in range(start_id, end_id + 1)]
    logs=[]
    logs_table.insert_multiple(logs)
    data_table.insert_multiple(customers)
    
    db.close()

def main():
    servers_info = []  # Used to store information of all servers
    # According to the requirements, 9 servers are divided into clusters:
    # S1 ~ S3 belong to Cluster 1, S4 ~ S6 belong to Cluster 2, S7 ~ S9 belong to Cluster 3
    for i in range(1, 10):
        server_id = f"S{i}"
        if i <= 3:
            cluster = 1
        elif i <= 6:
            cluster = 2
        else:
            cluster = 3
        
        # Assign an example IP address for each server (can be modified as needed)
        ip_address = "127.0.0.1"
        port = f"500{i}"

        # Database file name, such as db_S1.json, db_S2.json, ...
        db_file = f"data/db_server_{server_id}.json"
        
        # Create the database file for the server and initialize two tables
        create_server_database(server_id, cluster, db_file)
        
        # Save the basic information of the server
        servers_info.append({
            "server_id": server_id,
            "cluster": cluster,
            "ip": ip_address,
            "port": port,
            "db_file": db_file
        })
    
    # Write all server information to a separate file servers_info.json
    with open("data/servers_info.json", "w") as f:
        json.dump(servers_info, f, indent=4)
    
    print("Initialization complete: 9 server database files and servers_info.json file created.")

if __name__ == "__main__":
    main()