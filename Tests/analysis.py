import basic_framework as bf

# A-1: Default Configuration Simulation
bf.simulate_scenario(servers=6, shards=4, shard_replicas=3)

# A-2: Increased Shard Replicas Simulation
bf.simulate_scenario(servers=6, shards=4, shard_replicas=7)

# A-3: Increased Servers and Shards Simulation
bf.simulate_scenario(servers=10, shards=6, shard_replicas=8)

# A-4:
import random

class DistributedDatabase:
    def __init__(self):
        # Initial configuration based on the task details
        self.servers = {"Server0": ["sh1", "sh2"], "Server1": ["sh3", "sh4"]}
        self.shards = {"sh1": "Server0", "sh2": "Server0", "sh3": "Server1", "sh4": "Server1"}
        self.failed_servers = []

    def remove_server(self, server_name):
        """Simulate server removal and adjust shard assignments."""
        if server_name in self.servers:
            # Mark server as failed
            self.failed_servers.append(server_name)
            
            # Remove shards from failed server
            shards_to_reallocate = self.servers.pop(server_name)
            for shard in shards_to_reallocate:
                self.shards.pop(shard, None)

            print(f"Server {server_name} removed. Reallocating its shards...")

            # Reallocate shards to existing servers
            for shard in shards_to_reallocate:
                available_servers = list(self.servers.keys())
                if available_servers:
                    new_server = random.choice(available_servers)
                    self.servers[new_server].append(shard)
                    self.shards[shard] = new_server
                    print(f"Shard {shard} reallocated to {new_server}.")
                else:
                    print("No available servers to reallocate shards.")
        else:
            print(f"Server {server_name} not found.")

    def add_server(self, server_name, shards):
        """Simulate adding a new server and optionally assign shards."""
        if server_name not in self.servers:
            self.servers[server_name] = shards
            for shard in shards:
                self.shards[shard] = server_name
            print(f"Server {server_name} added with shards: {shards}")
        else:
            print(f"Server {server_name} already exists.")

    def spawn_new_server(self):
        """Simulate the load balancer response to spawn a new server container."""
        new_server_name = f"Server{len(self.servers) + len(self.failed_servers)}"
        self.add_server(new_server_name, [])
        print(f"New server {new_server_name} spawned by load balancer.")

    def display_configuration(self):
        """Display the current server and shard configuration."""
        print("\nCurrent Configuration:")
        for server, shards in self.servers.items():
            print(f"{server}: {shards}")
        print("Failed Servers:", self.failed_servers)

# Simulating Task A-4
database = DistributedDatabase()
database.display_configuration()



