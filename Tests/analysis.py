import basic_framework as bf

# A-1: Default Configuration Simulation
bf.simulate_scenario(servers=6, shards=4, shard_replicas=3)

# A-2: Increased Shard Replicas Simulation
bf.simulate_scenario(servers=6, shards=4, shard_replicas=7)

# A-3: Increased Servers and Shards Simulation
bf.simulate_scenario(servers=10, shards=6, shard_replicas=8)


