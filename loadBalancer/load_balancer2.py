from flask import Flask, jsonify, request
import os 
import logging 
import random
import requests
import threading
import time
import helper
from ConsistentHashmap import ConsistentHashmapImpl

app = Flask(__name__)

replicas = []

N = 3
currentNumberofServers = 0
serverName = []
servers = {}
virtualServers = 9
slotsInHashMap = 512
consistentHashMap = ConsistentHashmapImpl([], virtualServers, slotsInHashMap)
counter = 1
max_servers = slotsInHashMap//virtualServers
server_hash = {}
server_ids = [0] * (max_servers+1)
# servers_lock = threading.Lock()

# def check_server_health(server_url):
#     try:
#         response = requests.get(f"{server_url}/heartbeat", timeout=2)
#         return response.status_code == 200
#     except requests.exceptions.RequestException:
#         return False

# def health_check():
#     global servers, currentNumberofServers
#     try:
#         while True:
#             time.sleep(5)
#             servers_copy = dict(servers)

#             for server_name, server_url in servers_copy.items():
#                 if not check_server_health(server_url[1]):
#                     id = servers_copy[server_name][0]
#                     logging.debug(f"Server : {server_name} is down. Removing from the pool.")
#                     os.system(f'sudo docker stop {server_name} && sudo docker rm {server_name}')
#                     # Checking if values in map and then deleting it
#                     if server_name in servers:
#                         del servers[server_name]
#                     if id in server_hash:
#                         del server_hash[id]
#                     removeServer(id)
#                     consistentHashMap.removeServer(id, server_name)
#                     currentNumberofServers -= 1

#             # Check if the number of running servers is less than 3
#             while currentNumberofServers < 3:
#                 x = getServerID()
#                 name = f"server{x}"
#                 port = 5000 + x
#                 logging.debug(f"Creating new Server :{name}")
#                 print(currentNumberofServers)
#                 logging.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
#                 helper.createServer(x, name, port)
#                 consistentHashMap.addServer(x, name)
#                 servers[name] = [x, f"http://{name}:5000/"]
#                 server_hash[x] = name
#                 currentNumberofServers+=1

#             time.sleep(5)
#     except Exception as e:
#         print("****************************************8**")
#         logging.exception(f"Exception in health_check: {e}")



# def start_health_check_thread():
#     health_check_thread = threading.Thread(target=health_check)
#     health_check_thread.daemon = True
#     health_check_thread.start()

#################################--------------------------------- New Endpoints ---------------------------------#################################

#################################--------------------------------- ############# ---------------------------------#################################
#################################--------------------------------- ############# ---------------------------------#################################
#################################--------------------------------- ############# ---------------------------------#################################
#################################--------------------------------- ############# ---------------------------------#################################
#################################--------------------------------- ############# ---------------------------------#################################

server_urls = {}
number_of_replicas = 3
shard_hash_maps = {}
shard_information = {}
server_schema = None
server_shard_mapping = {}
server_id_to_name = {}
shard_locks = {}
ports = {}
def get_random_ports():
    counter = 0 
    while True and counter < 10000: 
        x = random.randint(1, 1000) 
        if x not in ports :
            ports[x] = 1 
            return 5000+x 
        counter+=1
    return -1

def remove_ports(id): 
    del ports[id] 


def get_random_server_id() : 
    return random.randint(100000, 999999)

def get_server_url(name): 
    return f"http://{name}:5000/"
    # using the following as container withing container for testing will take a lot of time 
    # return f"http://10.0.2.15:5000/"

def get_shard_id_from_stud_id(id):
    for shardId, info in shard_information.items(): 
        if id >= info['Stud_id_low'] and id < (info['Stud_id_low'] + info['Shard_size']):
            return shardId, info['Stud_id_low'] + info['Shard_size']
    return None, None

# 1) ShardT (Stud id low: Number, Shard id: Number, Shard size:Number, valid idx:Number)
# 2) MapT (Shard id: Number, Server id: Number)


# TODO 
current_configuration = {
    "N" : 0, 
    "schema" : {}, 
    "shards" : [], 
    "servers" : {}
}

# TODO : Need to add all sorts of checks as endpoint is build to blindly accept all requests. 

@app.route('/init', methods=['POST'])
def initialize_database():
    data = request.json  
    N = data.get('N')
    server_schema = data.get('schema')
    shards = data.get('shards')
    servers = data.get('servers')
    print(N, servers)

    # Update the current_configuration for status endpoint
    current_configuration['N']+=int(N)
    current_configuration['schema'] = server_schema
    current_configuration['shards'].extend(shards)

    for item in shards : 
        shard_information[item['Shard_id']] = item
        shard_information[item['Shard_id']]['valid_idx'] = 0

    for k, v in servers.items(): 
        current_configuration['servers'][k] = v
        random_server_id = get_random_server_id()
        name = k 
        if '$' in k : 
            name = f"Server{random_server_id}"
        helper.createServer(random_server_id, name, get_random_ports())
        while True:
            try : 
                response = requests.get(f"{get_server_url(name)}home")
                if response.status_code == 200 :
                    break
            except Exception as e:
                time.sleep(30)
                continue

        server_hash[name] = random_server_id
        server_id_to_name[random_server_id] = name
        requests.post(f"{get_server_url(name)}config", json={
            'schema': server_schema,
            'shards': v
        })
        for shard_id in v:
            if shard_id not in shard_hash_maps :
                shard_hash_maps[shard_id] = ConsistentHashmapImpl([], virtualServers, slotsInHashMap)
            shard_hash_maps[shard_id].addServer(random_server_id, name)
            if k not in server_shard_mapping : 
                server_shard_mapping[k] = []
            server_shard_mapping[k].append(shard_id)

    response_json = {
        "message": "Configured Database",
        "status": "success"
    }
    return jsonify(response_json), 200

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify(current_configuration), 200


@app.route('/add', methods=['POST'])
def add_servers():
    data = request.json
    N = data.get('n')
    shards = data.get('new_shards')
    servers = data.get('servers')
    message = "Added "
    
    if len(servers) < N or N < 0 :  
        return {"message": f"Number of new servers {N} is greater than newly added instances", 
                "status" : "failure"}, 400

    # Update the current_configuration for status endpoint
    current_configuration['N']+=int(N)
    current_configuration['shards'].extend(shards)


    for item in shards :
        shard_information[item['Shard_id']] = item
        shard_information[item['Shard_id']]['valid_idx'] = 0

    for k, v in servers.items(): 
        current_configuration['servers'][k] = v
        random_server_id = get_random_server_id() 
        name = k 
        if '$' in k : 
            name = f"Server{random_server_id}"
        message+=f"{name} "
        helper.createServer(random_server_id, name, get_random_ports())
        while True:
            try : 
                response = requests.get(f"{get_server_url(name)}home")
                if response.status_code == 200 :
                    break
            except Exception as e:
                time.sleep(30)
                continue

        server_hash[name] = random_server_id
        server_id_to_name[random_server_id] = name
        requests.post(get_server_url(name), json={
            'schema': server_schema,
            'shards': v
        })
        for shard_id in v:
            if shard_id not in shard_hash_maps : 
                shard_hash_maps[shard_id] = ConsistentHashmapImpl([], virtualServers, slotsInHashMap)
            shard_hash_maps[shard_id].addServer(random_server_id, name)
            if k not in server_shard_mapping : 
                server_shard_mapping[k] = []
            server_shard_mapping[k].append(shard_id)

    response_message = {
        "message" : message.strip(),
        "status" : "successful"
    }
    return jsonify(response_message), 200

@app.route('/rm', methods=['DELETE'])
def remove():
    data = request.json  
    N = data.get('n')
    servers = data.get('servers')

    # Sanity Checks 
    if len(servers) > N or N >= len(server_hash): 
        return jsonify({"message" : "Length of server list is more than removable instances",
                        "status" : "failure"}), 400
    
    for server in servers : 
        if server not in server_hash : 
            return jsonify({"message" : f"Server Name : {server} not found",
                        "status" : "failure"}), 400
    
    # Server Removal 
    for server in servers : 
        for shard_id in server_shard_mapping[server] : 
            shard_hash_maps[shard_id].removeServer(server_hash[server], server)
        del server_shard_mapping[server]
        del server_hash[server]
        N-=1

    while N != 0: 
        random_server = random.choice(list(server_hash.keys()))
        for shard_id in server_shard_mapping[random_server] : 
            shard_hash_maps[shard_id].removeServer(server_hash[random_server], random_server)
        del server_shard_mapping[random_server]
        del server_hash[random_server]
        N-=1
    
    return jsonify({'message': 'Removal successful'}), 200

@app.route('/read', methods=['POST'])
def read():
    data = request.json
    stud_id_low = data.get('Stud_id', {}).get('low')
    stud_id_high = data.get('Stud_id', {}).get('high')
    start_id = stud_id_low
    shards_queried = []
    while True : 
        shard_id, end_id = get_shard_id_from_stud_id(start_id)
        if end_id <= stud_id_high: 
            shards_queried.append([shard_id, start_id, end_id])
            break
        shards_queried.append([shard_id, start_id, end_id])
        start_id = end_id
        
    data_entries = []
    for shard_id, start_id, end_id in shards_queried:
        request_id = random.randint(100000, 999999)
        load_balancer_url = f"{get_server_url(server_id_to_name[shard_hash_maps[shard_id].getContainerID(request_id)])}read"
        payload = {
            'Stud_id': {'low': start_id, 'high': end_id} 
        }
        response = requests.post(load_balancer_url, json=payload)
        if response.status_code == 200:
            response_json = response.json()
            data_entries.extend(response_json.get('data', []))
        else:
            print("Failed to get response from load balancer. Status code:", response.status_code)
    return jsonify({'shards_queried': shards_queried, 'data': data_entries, 'status': 'success'}), 200


@app.route('/write', methods=['POST'])
def write():
    global database_config, shard_locks
    data_entries = request.json.get('data', [])

    shard_queries = {}
    entries_added = 0

    for entry in data_entries: 
        stud_id = entry.get('Stud_id')
        shard_id, end_id = get_shard_id_from_stud_id(stud_id)
        if shard_id is not None : 
            if shard_id not in shard_queries :
                shard_queries[shard_id] = []
            shard_queries[shard_id].append(entry)
    
    for shard_id, entry in shard_queries:
        shard_lock = shard_locks.setdefault(shard_id, threading.Lock())
        shard_lock.acquire()
        try:
            server_list = shard_hash_maps[shard_id].getServers()
            curr_idx = shard_information[shard_id]['valid_idx']
            for serverName in server_list :
                load_balancer_url = f"{get_server_url(serverName)}write"
                payload = {
                    'shard': shard_id, 'curr_idx': curr_idx, "data" : entry 
                }
                response = requests.post(load_balancer_url, json=payload)
                if response.status_code == 200:
                    response_json = response.json()
                    shard_information[shard_id]['valid_idx'] = response_json.get('current_idx')
                    entries_added+=(shard_information[shard_id]['valid_idx'] - curr_idx)
                else:
                    print("Failed to get response from load balancer. Status code:", response.status_code)
        finally:
            shard_lock.release()

    return jsonify({'message': f"{entries_added} Data entries added", 'status': 'success'}), 200



@app.route('/update', methods=['PUT'])
def update():
    data_entry = request.json.get('data', {})
    stud_id = request.json.get('stud_id', {})
    shard_id, end_id = get_shard_id_from_stud_id(stud_id)
    if shard_id is not None : 
        shard_lock = shard_locks.setdefault(shard_id, threading.Lock())
        shard_lock.acquire()
        message = ""
        code = 200
        try:
            server_list = shard_hash_maps[shard_id].getServers()
            for serverName in server_list :
                if code == 400 :
                    break
                load_balancer_url = f"{get_server_url(serverName)}write"
                payload = {
                    'shard': shard_id, 'Stud_id': stud_id, "data" : data_entry 
                }
                response = requests.post(load_balancer_url, json=payload)
                if response.status_code == 200:
                    message =  f"Data entry for Stud_id: {stud_id} updated"
                else:
                    print("Failed to get response from load balancer. Status code:", response.status_code)
                    message = 'Update Unsuccessful'
                    code = 400
        finally:
            shard_lock.release()
        return jsonify({'message': message, 'status' : "successful"}), code
    return jsonify({'message': 'Update Unsuccessful'}), 400 

@app.route('/del', methods=['DELETE'])
def delete():
    stud_id = request.json.get('stud_id', {})
    shard_id, end_id = get_shard_id_from_stud_id(stud_id)
    if shard_id is not None : 
        shard_lock = shard_locks.setdefault(shard_id, threading.Lock())
        shard_lock.acquire()
        message = ""
        code = 200
        try:
            server_list = shard_hash_maps[shard_id].getServers()
            for serverName in server_list :
                if code == 400 :
                    break
                load_balancer_url = f"{get_server_url(serverName)}write"
                payload = {
                    'shard': shard_id, 'Stud_id': stud_id 
                }
                response = requests.post(load_balancer_url, json=payload)
                if response.status_code == 200:
                    message =  f"Data entry with Stud_id: {stud_id} removed from all replicas"
                else:
                    print("Failed to get response from load balancer. Status code:", response.status_code)
                    message = 'Update Unsuccessful'
                    code = 400
        finally:
            shard_lock.release()
        return jsonify({'message': message, 'status' : "successful"}), code
    return jsonify({'message': 'Update Unsuccessful'}), 400 



# Error handling
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Not found'}), 404

if __name__ =='__main__':
    # logging.debug(os.popen("sudo docker rm -f  $(sudo docker ps -aq)").read())
    # try:
    #     logging.info(os.popen(f"sudo docker network create my_network").read())
    # except:
    #     logging.info("Network my_network already exists.")
    # for i in range(1, N+1):
    #     x = getServerID()
    #     name = f"server{x}"
    #     port = 5000 + x
    #     helper.createServer(x, name, port)
    #     consistentHashMap.addServer(x, name)
    #     servers[name] = [x, f"http://{name}:5000/"]
    #     print(servers[name])
    #     server_hash[x] = name
    #     currentNumberofServers+=1

    # start_health_check_thread()
    print(os.popen(f"sudo docker rm my_network").read())
    print(os.popen(f"sudo docker network create my_network").read())
    app.run(host="0.0.0.0", port=5000, threaded=True)