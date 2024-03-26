
# Distributed Database Simulation README

## Overview

This README explains how to run the Python code simulations for tasks A-1 to A-4 of the distributed database analysis. These tasks simulate the performance and behavior of a sharded, distributed database system under various configurations.

## Prerequisites

- Python 3.6 or newer installed on your system.
- Basic understanding of command line operations and Python execution.

## Running the Simulations

### A-1 to A-3: Performance Analysis

The Python code for tasks A-1 to A-3 simulates the read and write performance of the distributed database under different configurations.

1. **Open your terminal or command prompt.**
2. **Navigate to the directory containing the simulation script.** Use the `cd` command to change directories.

    ```bash
    cd path/to/simulation_script
    ```

3. **Run the simulation script with Python.**

    ```bash
    python3 analysis.py
    ```

   This command will execute the simulations for tasks A-1 to A-3 sequentially and display the results for each task, including the write and read times for 10,000 operations under different server and shard configurations.

### A-4: Endpoint Correctness and Server Drop Simulation

Task A-4's code simulates the behavior of the system's endpoints, particularly focusing on simulating a server drop and how the load balancer responds by spawning a new server and reallocating shards.

1. **Continue in the terminal or command prompt where you are.**
2. **Ensure you are still in the directory containing the simulation script for A-4.**
3. **Run the script for Task A-4.**

    ```bash
    python3 analysis.py
    ```

   Replace `simulate_a4.py` with the actual name of your Python script for Task A-4.

   This script will demonstrate removing a server, showing how shards are reallocated to remaining servers, and then simulating the load balancer's response by adding a new server.

## Interpreting the Results

- **For A-1 to A-3:** The script outputs the time taken for 10,000 write and read operations under different configurations. It will help you understand the impact of increasing shard replicas and servers on the system's performance.

- **For A-4:** The script demonstrates the system's resilience to server failures by automatically reallocating shards and spawning new servers. It will output the steps taken during the simulation, including server removal, shard reallocation, and new server addition.

## Additional Notes

- These simulations are simplified and do not represent the full complexity of a real distributed database system. They are intended for educational purposes to demonstrate basic concepts of distributed database design and load balancing.

- The performance results from A-1 to A-3 are theoretical and depend on the assumptions made in the simulation code. Actual database performance can vary based on network latency, database engine, and hardware capabilities.

---

Save this README with your Python scripts to provide users with the necessary guidance on running and understanding the simulations.
