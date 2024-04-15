import time
import requests
import random
import string
import asyncio
import aiohttp

def get_write_tasks(session):
    tasks = []
    load_balancer_url_write = "http://127.0.0.1:5000/write"
    for i in range(1, 10001):
        payload = {
            "data": [
                {
                    "Stud_id": i,
                    "Stud_name": generate_random_string(5),
                    "Stud_marks": random.randint(1, 100)
                }
            ]
        }

        tasks.append(asyncio.create_task(session.post(load_balancer_url_write, json=payload)))

    return tasks

def get_read_tasks(session):
    tasks = []
    load_balancer_url_read = "http://127.0.0.1:5000/read"
    for i in range(1, 10001):
        payload = {
            "Stud_id": {"low": i, "high": i}
        }

        tasks.append(asyncio.create_task(session.post(load_balancer_url_read, json=payload)))

    return tasks

async def measure_write_speed_async():
    async with aiohttp.ClientSession() as session:
        write_tasks = get_write_tasks(session)
        responses = await asyncio.gather(*write_tasks)

        for i, response in enumerate(responses):
            if response.status != 200:
                print(f"Error in write request {i}")

async def measure_read_speed_async():
    async with aiohttp.ClientSession() as session:
        read_tasks = get_read_tasks(session)
        responses = await asyncio.gather(*read_tasks)

        for i, response in enumerate(responses):
            if response.status != 200:
                print(f"Error in read request {i}")


# Function to measure the time taken for 10000 writes
def measure_write_speed(url, payload):
    start_time = time.time()

    # Send the request one by one to measure the time taken for 10000 writes
    for i in range(1, 10001):
        payload["data"][0]["Stud_id"] = i
        payload["data"][0]["Stud_name"] = generate_random_string(5)
        payload["data"][0]["Stud_marks"] = random.randint(1, 100)

        response = requests.post(url, json=payload)

        if response.status_code != 200:
            print(f"Error in write request {i}")

    end_time = time.time()
    write_time = end_time - start_time
    return write_time

def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

# Function to measure the time taken for 10000 reads
def measure_read_speed(url, payload):
    start_time = time.time()

    # Send the request one by one to measure the time taken for 10000 reads
    for i in range(1, 10001):
        payload["Stud_id"]["low"] = i
        payload["Stud_id"]["high"] = i

        response = requests.post(url, json=payload)

        if response.status_code != 200:
            print(f"Error in read request {i}")

    end_time = time.time()
    read_time = end_time - start_time
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
    write_data.append({
        "Stud_id": 1,
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
