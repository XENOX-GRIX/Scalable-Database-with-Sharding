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

2. **Install MySQL** (Optional)  
   - This step is optional and needed if you intend to run MySQL directly on your host for development purposes. Follow the instructions in [MySQL's official documentation](https://dev.mysql.com/doc/mysql-installation-excerpt/8.0/en/).

3. **Prepare the Python Environment**  
   - Set up a Python virtual environment and install dependencies:
     ```sh
     python3 -m venv venv
     source venv/bin/activate
     pip install --upgrade pip
     pip install -r requirements.txt
     ```

### Running the Application

1. **Build Docker Containers**  
   - The system components (servers, load balancer) are containerized. Use Docker Compose to build them:
     ```sh
     docker-compose build
     ```

2. **Initialize the Database**  
   - A script is provided to initialize the database schema before running the application:
     ```sh
     docker-compose run db-init
     ```

3. **Start the System**  
   - Launch the system components including the database, servers, and load balancer:
     ```sh
     docker-compose up
     ```

4. **Interact with the System**  
   - The system provides a series of endpoints for interaction. Example scripts are provided for common operations, such as initializing the load balancer:
     ```sh
     python scripts/init_load_balancer.py
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
