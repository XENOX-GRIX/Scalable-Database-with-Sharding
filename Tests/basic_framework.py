import time
import random

# Base simulation class
class DistributedDatabaseSimulation:
    def __init__(self, servers, shards, shard_replicas):
        self.servers = servers
        self.shards = shards
        self.shard_replicas = shard_replicas

    def simulate_write(self, operations):
        start_time = time.time()
        # Simulate write delay based on shards and shard_replicas
        for _ in range(operations):
            # Assuming constant time operation for simplification
            pass
        end_time = time.time()
        return end_time - start_time

    def simulate_read(self, operations):
        start_time = time.time()
        # Simulate read delay based on servers and shard_replicas
        for _ in range(operations):
            # Assuming constant time operation for simplification
            pass
        end_time = time.time()
        return end_time - start_time

# Function to simulate scenarios
def simulate_scenario(servers, shards, shard_replicas, operations=10000):
    db_sim = DistributedDatabaseSimulation(servers, shards, shard_replicas)
    write_time = db_sim.simulate_write(operations)
    read_time = db_sim.simulate_read(operations)
    print(f"Simulation with {servers} servers, {shards} shards, {shard_replicas} replicas")
    print(f"Write Time for {operations} operations: {write_time} seconds")
    print(f"Read Time for {operations} operations: {read_time} seconds")
