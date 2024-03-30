import time
import requests
import random
import string

# Function to measure the time taken for 10000 writes
def measure_write_speed(url, payload):
    start_time = time.time()
    response = requests.post(url, json=payload)
    end_time = time.time()
    write_time = end_time - start_time
    # print(response.status_code)
    # print(response.text)
    return write_time

def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

# Function to measure the time taken for 10000 reads
def measure_read_speed(url, payload):
    start_time = time.time()
    response = requests.post(url, json=payload)
    end_time = time.time()
    read_time = end_time - start_time
    # print(response.status_code)
    # print(response.text)
    return read_time

# Test function for Task A-1
def test_a1():
    load_balancer_url_init = "http://127.0.0.1:5000/init" 
    load_balancer_url_write = "http://127.0.0.1:5000/write"  
    load_balancer_url_read = "http://127.0.0.1:5000/read"  


    # increase the number of Servers (to 10) by adding new servers and increase the number of shards (shard to 6, shard
    # replicas to 8). Define the (init endpoint) configurations according to your choice. Report the write speed up for 10000
    # writes and read speed up for 10000 reads.

    init_payload = {
        "N":10,
        "schema":{
            "columns":[
                "Stud_id",
                "Stud_name",
                "Stud_marks"
            ],
            "dtypes":[
                "Number",
                "String",
                "String"
            ]
        },
        "shards":[
            {
                "Stud_id_low":0,
                "Shard_id":"sh1",
                "Shard_size":4096
            },
            {
                "Stud_id_low":4096,
                "Shard_id":"sh2",
                "Shard_size":4096
            },
            {
                "Stud_id_low":8192,
                "Shard_id":"sh3",
                "Shard_size":4096
            },
            {
                "Stud_id_low":12288,
                "Shard_id":"sh4",
                "Shard_size":4096
            },
            {
                "Stud_id_low":16384,
                "Shard_id":"sh5",
                "Shard_size":4096
            },
            {
                "Stud_id_low":20480,
                "Shard_id":"sh6",
                "Shard_size":4096
            }
        ],
        "servers":{
            "Server0":[
                "sh1",
                "sh2",
                "sh3"
            ],
            "Server1":[
                "sh1",
                "sh2",
                "sh3"
            ],
            "Server2":[
                "sh1",
                "sh2",
                "sh3", 
                "sh4", 
                "sh5", 
                "sh6"
            ],
            "Server3":[
                "sh1",
                "sh2",
                "sh3", 
                "sh4", 
                "sh5", 
                "sh6"
                
            ],
            "Server4":[
                "sh1",
                "sh2",
                "sh3", 
                "sh4", 
                "sh5", 
                "sh6"
            ],
            "Server5":[
                "sh1",
                "sh2",
                "sh3",
                "sh4",
                "sh5", 
                "sh6"
            ],
            "Server6":[
                "sh4",
                "sh5",
                "sh6", 
                "sh1",
                "sh2",
                "sh3"
            ],
            "Server7":[
                "sh4",
                "sh5",
                "sh6",
                "sh1", 
                "sh2",
                "sh3"
            ],
            "Server8":[
                "sh4",
                "sh5",
                "sh6"
            ],
            "Server9":[
                "sh6",
                "sh5",
                "sh4"
            ]
        }
    }

    while True : 
        try : 
            response = requests.post(load_balancer_url_init, json=init_payload)
            if response.status_code == 200 : 
                break
        except : 
            time.sleep(30)
    # Payload for write endpoint (10000 entries)
    write_data = []
    for i in range(1,10001) : 
        write_data.append({
            "Stud_id": i, 
            "Stud_name": generate_random_string(5), 
            "Stud_marks": random.randint(1, 100) 
        })
    payload_write = {
        "data": write_data
    }
    payload_read = {
        "Stud_id": {"low": 0, "high": 10000}
    }

    # Measure write speed
    write_time = measure_write_speed(load_balancer_url_write, payload_write)
    print(f"Time taken for 10,000 writes: {write_time} seconds")

    # Measure read speed
    read_time = measure_read_speed(load_balancer_url_read, payload_read)
    print(f"Time taken for 10,000 reads: {read_time} seconds")

# Run the test
test_a1()
