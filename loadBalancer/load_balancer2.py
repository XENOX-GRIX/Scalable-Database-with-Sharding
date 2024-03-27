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
serverName = []
virtualServers = 9
slotsInHashMap = 512
consistentHashMap = ConsistentHashmapImpl([], virtualServers, slotsInHashMap)
init_called = 0
max_servers = slotsInHashMap//virtualServers
server_ids = [0] * (max_servers+1)


#################################--------------------------------- New Endpoints ---------------------------------#################################
#################################--------------------------------- ############# ---------------------------------#################################
#################################--------------------------------- ############# ---------------------------------#################################
#################################--------------------------------- ############# ---------------------------------#################################
#################################--------------------------------- ############# ---------------------------------#################################
#################################--------------------------------- ############# ---------------------------------#################################

number_of_replicas = 2
shard_hash_maps = {}
shard_information = {}
server_schema = None
server_shard_mapping = {}
server_id_to_name = {}
server_name_to_id = {}
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
    # if name == 'server3':
    #     return f"http://10.0.2.15:5003/"
    # if name == 'server1':
    #     return f"http://10.0.2.15:5001/"
    # if name == 'server2':
    #     return f"http://10.0.2.15:5002/"
    
    return f"http://{name}:5000/"


def check_server_health(server_url):
    try:
        response = requests.get(f"{server_url}heartbeat", timeout=2)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def health_check():
    try:
        while True:
            time.sleep(5)
            servers_copy = dict(server_shard_mapping)
            down_servers = []
            for server_name, shard_list in servers_copy.items():
                if not check_server_health(get_server_url(server_name)):
                    print(f"Server : {server_name} is down. Removing from the pool.")
                    down_servers.append(server_name) 
                    try :
                        os.system(f'sudo docker stop {server_name} && sudo docker rm -f {server_name}')
                    except :
                        print("Server removed...") 

            # Check if the number of running servers is less than 3
            # print(f"down Servers : {down_servers}")
            for name in down_servers :
                helper.createServer(server_name_to_id[name], name, get_random_ports())
                shard_ids = server_shard_mapping[name] 
                while True :
                    try : 
                        response = requests.post(f"{get_server_url(name)}config", json={
                            'schema': server_schema,
                            'shards': shard_ids
                        })
                        if response.status_code == 200 : 
                            print(f"Server {name} Spawned successfully")
                            break
                    except : 
                        time.sleep(30)
                try : 
                    shards_queried_ = {}
                    for i in shard_ids : 
                        shards_queried_[i] = 0
                    servers_shards  = {}
                    for id_ in shard_ids :
                        server_list_ = set(shard_hash_maps[id_].getServers())
                        for s1 in server_list_ : 
                            if s1 not in down_servers:
                                if s1 not in servers_shards : 
                                    servers_shards[s1] = []
                                servers_shards[s1].append(id_)
                    servers_shards = dict(sorted(servers_shards.items(), key=lambda x: len(x[1]), reverse=True))         
                    for k, v in servers_shards.items() :
                        val = []
                        for i in v : 
                            if shards_queried_[i] == 0 : 
                                val.append(i)
                                shards_queried_[i] = 1
                        if len(val) == 0 : 
                            continue
                        response = requests.get(f"{get_server_url(k)}copy", json={
                            'shards': val
                        })
                        for shard_id in val:
                            entry = response.json().get(shard_id, [])
                            shard_lock = shard_locks.setdefault(shard_id, threading.Lock())
                            shard_lock.acquire()
                            try:
                                load_balancer_url = f"{get_server_url(name)}write"
                                payload = {
                                    'shard': shard_id, 'curr_idx': 0, "data" : entry
                                }
                                response = requests.post(load_balancer_url, json=payload)
                            finally:
                                shard_lock.release()
                except Exception as e : 
                    print(e)
                print(f"New Server created : {name}")
            time.sleep(5)
    except Exception as e:
        print("******************************************")
        print(e)

def start_health_check_thread():
    health_check_thread = threading.Thread(target=health_check)
    health_check_thread.daemon = True
    health_check_thread.start()

def get_shard_id_from_stud_id(id):
    for shardId, info in shard_information.items(): 
        if id >= int(info['Stud_id_low']) and id < (int(info['Stud_id_low']) + int(info['Shard_size'])):
            return shardId, int(info['Stud_id_low']) + int(info['Shard_size'])
    return None, None

current_configuration = {
    "N" : 0, 
    "schema" : {}, 
    "shards" : [], 
    "servers" : {}
}


@app.route('/init', methods=['POST'])
def initialize_database():
    global init_called, server_schema
    message = "Configured Database"
    status = "Successful"
    if init_called == 1 :
        response_json = {
            "message": "Cannot initialize the server configurations more than once ", 
            "status" : "Unsuccessful"
        }
        return jsonify(response_json), 400
    init_called+=1
    try : 
        data = request.json  
        N = data.get('N')
        server_schema = data.get('schema')
        shards = data.get('shards')
        servers = data.get('servers')


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
            server_id_to_name[random_server_id] = name
            server_name_to_id[name] = random_server_id
            
            while True:
                try : 
                    response = requests.post(f"{get_server_url(name)}config", json={
                        'schema': server_schema,
                        'shards': v
                    })
                    if response.status_code == 200 :
                        print(f"Successfully created Server : {name}")
                        print(response.text)
                        break
                except Exception as e:
                    time.sleep(30)
                    continue

            for shard_id in v:
                if shard_id not in shard_hash_maps :
                    shard_hash_maps[shard_id] = ConsistentHashmapImpl([], virtualServers, slotsInHashMap)
                shard_hash_maps[shard_id].addServer(random_server_id, name)
                if name not in server_shard_mapping : 
                    server_shard_mapping[name] = []
                server_shard_mapping[name].append(shard_id)



    except Exception as e : 
        message = e
        status = "Unsuccessful"

    response_json = {
        "message": message,
        "status": status
    }
    start_health_check_thread()
    return jsonify(response_json), 200

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify(current_configuration), 200


@app.route('/add', methods=['POST'])
def add_servers():
    try : 
        global server_schema
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
            server_id_to_name[random_server_id] = name
            server_name_to_id[name] = random_server_id
            
            while True:
                try : 
                    response = requests.post(f"{get_server_url(name)}config", json={
                        'schema': server_schema,
                        'shards': v
                    })
                    if response.status_code == 200 :
                        print(f"Successfully created Server : {name}")
                        print(response.text)
                        break
                except Exception as e:
                    print(e)
                    time.sleep(30)
                    continue

            for shard_id in v:
                if shard_id not in shard_hash_maps : 
                    shard_hash_maps[shard_id] = ConsistentHashmapImpl([], virtualServers, slotsInHashMap)
                shard_hash_maps[shard_id].addServer(random_server_id, name)
                if name not in server_shard_mapping : 
                    server_shard_mapping[name] = []
                server_shard_mapping[name].append(shard_id)

        response_message = {
            "message" : message.strip(),
            "status" : "successful"
        }
        return jsonify(response_message), 200
    except Exception as e : 
        print(e) 
        return jsonify({'message': 'Add Unsuccessful'}), 400

@app.route('/rm', methods=['DELETE'])
def remove():
    try : 
        data = request.json  
        N = data.get('n')
        servers = data.get('servers')

        # Sanity Checks 
        if len(servers) > N or N >= len(server_name_to_id): 
            return jsonify({"message" : "Length of server list is more than removable instances",
                            "status" : "failure"}), 400
        
        for server in servers : 
            if server not in server_name_to_id.keys() : 
                return jsonify({"message" : f"Server Name : {server} not found",
                            "status" : "failure"}), 400
        
        # Server Removal 
        for server in servers : 
            for shard_id in server_shard_mapping[server] : 
                shard_hash_maps[shard_id].removeServer(server_name_to_id[server], server)
            del server_shard_mapping[server]
            del server_name_to_id[server]
            N-=1

        while N != 0: 
            random_server = random.choice(list(server_name_to_id.keys()))
            for shard_id in server_shard_mapping[random_server] : 
                shard_hash_maps[shard_id].removeServer(server_name_to_id[random_server], random_server)
            del server_shard_mapping[random_server]
            del server_name_to_id[random_server]
            N-=1
        
        return jsonify({'message': 'Removal successful'}), 200
    except Exception as e : 
        print(e)
        return jsonify({'message': 'Removal Unsuccessful'}), 400
        


@app.route('/read', methods=['POST'])
def read():
    try : 
        data = request.json
        stud_id_low = int(data.get('Stud_id', {}).get('low'))
        stud_id_high = int(data.get('Stud_id', {}).get('high'))
        start_id = stud_id_low
        shards_queried = []

        while True :
            shard_id, end_id = get_shard_id_from_stud_id(int(start_id))
            print(start_id, end_id, shard_id)
            if end_id is None or end_id > stud_id_high:
                if end_id is not None and stud_id_high > start_id: 
                    shards_queried.append([shard_id, start_id, end_id])
                break
            shards_queried.append([shard_id, start_id, end_id])
            start_id = end_id
            
        data_entries = []
        for shard_id, start_id, end_id in shards_queried:
            request_id = random.randint(100000, 999999)
            load_balancer_url = f"{get_server_url(server_id_to_name[shard_hash_maps[shard_id].getContainerID(request_id)])}read"
            payload = {
                'shard' : shard_id,
                'Stud_id': {'low': start_id, 'high': end_id} 
            }
            response = requests.post(load_balancer_url, json=payload)
            if response.status_code == 200:
                response_json = response.json()
                data_entries.extend(response_json.get('data', []))
            else:
                print(response.text)
                print("Failed to get response from load balancer. Status code:", response.status_code)
                return jsonify({'status': 'Unsuccessful'}), 400
        return jsonify({'shards_queried': shards_queried, 'data': data_entries, 'status': 'success'}), 200
    except Exception as e : 
        print(e)
        return jsonify({'message': "Failes to read", 'status': 'Unsuccessful'}), 400


@app.route('/write', methods=['POST'])
def write():
    try : 
        global shard_locks
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
        
        for shard_id, entry in shard_queries.items():
            shard_lock = shard_locks.setdefault(shard_id, threading.Lock())
            shard_lock.acquire()
            try:
                server_list = shard_hash_maps[shard_id].getServers()
                curr_idx = int(shard_information[shard_id]['valid_idx'])
                tried = 0 
                for serverName in server_list :
                    load_balancer_url = f"{get_server_url(serverName)}write"
                    payload = {
                        'shard': shard_id, 'curr_idx': curr_idx, "data" : entry 
                    }
                    response = requests.post(load_balancer_url, json=payload)
                    if response.status_code == 200:
                        response_json = response.json()
                        shard_information[shard_id]['valid_idx'] = int(response_json.get('current_idx'))
                        if tried == 0 :
                            entries_added+=(shard_information[shard_id]['valid_idx'] - curr_idx)
                            tried+=1 
                    else:
                        print(response.text)
                        print("Failed to get response from load balancer. Status code:", response.status_code)
                        return jsonify({'message': f"Failed to get response from load balancer. Status code:{response.status_code}", 'status': 'Unsuccessful'}), 400
            finally:
                shard_lock.release()
        return jsonify({'message': f"{entries_added} Data entries added", 'status': 'success'}), 200
    except Exception as e : 
        print(e) 
        return jsonify({'message': f"Failed to get response from load balancer", 'status': 'Unsuccessful'}), 400

@app.route('/update', methods=['PUT'])
def update():
    try : 
        data_entry = request.json.get('data', {})
        stud_id = request.json.get('Stud_id')
        shard_id, end_id = get_shard_id_from_stud_id(int(stud_id))
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
                    load_balancer_url = f"{get_server_url(serverName)}update"
                    payload = {
                        'shard': shard_id, 'Stud_id': stud_id, "data" : data_entry 
                    }
                    response = requests.put(load_balancer_url, json=payload)
                    if response.status_code == 200:
                        message =  f"Data entry for Stud_id: {stud_id} updated"
                    else:
                        print("Failed to get response from load balancer. Status code:", response.status_code)
                        print(response.text)
                        message = 'Update Unsuccessful'
                        code = 400
            finally:
                shard_lock.release()
            return jsonify({'message': message, 'status' : "successful"}), code
        return jsonify({'message': 'Update Unsuccessful'}), 400 
    except Exception as e :
        print(e)
        return jsonify({'message': 'Update Unsuccessful'}), 400 


@app.route('/del', methods=['DELETE'])
def delete():
    try : 
        stud_id = request.json.get('Stud_id')
        shard_id, end_id = get_shard_id_from_stud_id(int(stud_id))
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
                    load_balancer_url = f"{get_server_url(serverName)}del"
                    payload = {
                        'shard': shard_id, 'Stud_id': stud_id 
                    }
                    response = requests.delete(load_balancer_url, json=payload)
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
    except Exception as e : 
        print(e)
        return jsonify({'message': 'Update Unsuccessful'}), 400 




# Error handling
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Not found'}), 404

if __name__ =='__main__':
    print(os.popen(f"sudo docker rm my_network").read())
    print(os.popen(f"sudo docker network create my_network").read())
    app.run(host="0.0.0.0", port=5000, threaded=True)
