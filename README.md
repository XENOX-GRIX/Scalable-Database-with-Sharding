# Scalable Sharded Database System

## Overview

This project demonstrates a scalable, sharded database system intended for educational purposes on distributed systems concepts. It features server implementation handling sharded data, a custom load balancer for request distribution, and analysis of system performance under various configurations.

## System Requirements

- **OS**: Ubuntu 20.04 LTS or above
- **Docker**: Version 20.10.23 or above
- **Python**: 3.6 or newer
- **MySQL**: Version 8.0

## Getting Started

### Setup

1. **Install Docker**  
   - Visit [Docker's official installation guide](https://docs.docker.com/engine/install/ubuntu/) and follow the instructions to install Docker on Ubuntu.

### Running the Application

1. **Build Docker Containers**
   ```
   # Without using docker-compose
   make run
   
   # With using docker-compose
   make run_compose
   ```
## Architecture

This scalable sharded database system is designed around the principles of distributed databases, specifically focusing on sharding and load balancing. 

- **Database Sharding**: Data is horizontally partitioned across multiple shards. Each shard holds a subset of the data, allowing for distributed queries and operations.
- **Load Balancer**: Implements consistent hashing to distribute read and write requests across shards efficiently, ensuring even load distribution and facilitating scalability.
- **Server Containers**: Each container simulates a database server managing a set of shards. Servers are orchestrated using Docker, enabling easy scaling and replication.

## Task Implementations

### Task 1: Server Implementation

Servers are designed to manage sharded data with the following endpoints:
- `/config`: Initializes shard configurations.
- `/heartbeat`: Provides server status.
- `/copy`, `/read`, `/write`, `/update`, `/del`: Handle data operations within shards.

### Task 2: Load Balancer Enhancement

The load balancer has been enhanced to support dynamic shard and server management, featuring endpoints for initializing configurations, adding/removing servers, and distributing read/write requests.

### Task 3: Performance Analysis

Performance analysis involved measuring read and write speeds under various configurations:
- **Default Configuration**: Demonstrated baseline performance.
- **Increased Shard Replicas**: Showed improved read speeds due to parallelism.
- **Increased Servers and Shards**: Highlighted scalability and its impact on performance.
- **Endpoint Correctness**: Validated through simulated server failures and automatic recovery.

### Tests

<p align="center">
      <img src="Tests/test1.png" width="90%"/><br><strong>Test-1 results for 10000 reads and 10000 writes</strong>
</p>

<p align="center">
      <img src="Tests/test2.png" width="90%"/><br><strong>Test-2 results for 10000 reads and 10000 writes (Write Speed-down = 2.14267270488, Read Speed-up = 1.08771929825)</strong>
</p>

<p align="center">
      <img src="Tests/test3.png" width="90%"/><br><strong>Test-3 results for 10000 reads and 10000 writes (Write Speed-down = 2.5979025146, Read Speed-up = 1.0350877193)</strong>
</p>




## Endpoints test results

### Server

#### Endpoint (/config, method=POST)
<p align="center">
      <img src="Tests/Server/server_config.png" width="90%"/><br><strong>Working of Server endpoint /config</strong>
</p>

#### Endpoint (/heartbeat, method=GET)
<p align="center">
      <img src="Tests/Server/server_heartbeat.png" width="90%"/><br><strong>Working of Server endpoint /heartbeat</strong>
</p>

#### Endpoint (/copy, method=GET)
<p align="center">
      <img src="Tests/Server/server_copy.png" width="90%"/><br><strong>Working of Server endpoint /copy</strong>
</p>

#### Endpoint (/read, method=POST)
<p align="center">
      <img src="Tests/Server/server_read.png" width="90%"/><br><strong>Working of Server endpoint /read</strong>
</p>

#### Endpoint (/write, method=POST)
<p align="center">
      <img src="Tests/Server/server_write.png" width="90%"/><br><strong>Working of Server endpoint /write</strong>
</p>

#### Endpoint (/update, method=PUT)
<p align="center">
      <img src="Tests/Server/server_update.png" width="90%"/><br><strong>Working of Server endpoint /update</strong>
</p>

#### Endpoint (/del, method=DELETE)
<p align="center">
      <img src="Tests/Server/server_del.png" width="90%"/><br><strong>Working of Server endpoint /del</strong>
</p>

### Load Balancer

#### Endpoint (/init, method=POST)
<p align="center">
      <img src="Tests/lb_init.png" width="90%"/><br><strong>Working of Load-balancer endpoint /init</strong>
</p>

#### Endpoint (/status, method=GET)
<p align="center">
      <img src="Tests/lb_ststus.png" width="90%"/><br><strong>Working of Load-balancer endpoint /status</strong>
</p>

#### Endpoint (/add, method=POST)
<p align="center">
      <img src="Tests/lb_add.png" width="90%"/><br><strong>Working of Load-balancer endpoint /add</strong>
</p>

#### Endpoint (/rm, method=DELETE)
<p align="center">
      <img src="Tests/lb_rm.png" width="90%"/><br><strong>Working of Load-balancer endpoint /rm</strong>
</p>

#### Endpoint (/read, method=POST)
<p align="center">
      <img src="Tests/lb_read.png" width="90%"/><br><strong>Working of Load-balancer endpoint /read</strong>
</p>

#### Endpoint (/write, method=POST)
<p align="center">
      <img src="Tests/lb_write.png" width="90%"/><br><strong>Working of Load-balancer endpoint /write</strong>
</p>

#### Endpoint (/update, method=PUT)
<p align="center">
      <img src="Tests/lb_update.png" width="90%"/><br><strong>Working of Load-balancer endpoint /update</strong>
</p>

#### Endpoint (/del, method=DELETE)
<p align="center">
      <img src="Tests/lb_del.png" width="90%"/><br><strong>Working of Load-balancer endpoint /del</strong>
</p>


#### heart_beat functionality 
<p align="center">
      <img src="Tests/lb_heartbeat.png" width="90%"/><br><strong>Heart-beat working</strong>
</p>
<p align="center">
      <img src="Tests/after_heartbeat_read.png" width="90%"/><br><strong>Heart-beat working</strong>
</p>
## Appendix

### Docker and MySQL Setup

The project leverages Docker to run MySQL alongside the Flask application, facilitating an isolated and replicable environment. The `Dockerfile` sets up MySQL and installs necessary Python dependencies, while `deploy.sh` initializes the Flask application.

### References

- Docker Documentation: https://docs.docker.com/
- MySQL Official Guide: https://dev.mysql.com/doc/
- Python Virtual Environments: https://docs.python.org/3/tutorial/venv.html

## Contributors

- Aakash Gupta
- Rajanyo Paul
- Avik Pramanick
- Soham Banerjee
