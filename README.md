# Distributed Auction System

## Overview
This project implements a distributed auction system that allows multiple clients to participate in auctions managed by a leader node. The system is designed to handle high availability and fault tolerance through a leader-worker architecture.

## Features
- **Node Role Selection**: Nodes can discover each other and elect a leader to manage auctions.
- **Leader Responsibilities**: The leader manages worker nodes, tracks active auctions, and handles client requests.
- **Worker Responsibilities**: Workers execute auction logic, handle bids, and communicate results back to the leader.
- **Auction Model**: Each auction is represented by an object that tracks its state, bids, and participants.

## Installation
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd prod-area-ds-auction-w25
   ```

## Usage
1. Start the one or multiple node:
   ```bash
   python node.py
   ```
2. Start client process:
   ```bash
   python worker.py
   ```



## Architectural Model
The distributed auction system employs a hybrid architectural model utilizing both TCP and UDP protocols. TCP is used for reliable communication between nodes, ensuring that messages such as auction bids and results are delivered accurately. UDP is utilized for broadcasting messages like auction updates to multiple participants simultaneously, allowing for faster communication without the overhead of establishing a connection.

## Discovery Mechanism
The discovery mechanism implemented in `node.py` allows nodes to find each other in the network. Each node periodically sends out discovery messages over UDP to announce its presence. Upon receiving a discovery message, nodes can register each other and maintain a list of active nodes, facilitating communication and coordination among them.

## Election of Leader
The leader election process is initiated when nodes detect that the current leader is unresponsive. Nodes use a consensus algorithm to elect a new leader based on their unique identifiers. The node with the highest identifier is chosen as the new leader, and it takes over the responsibilities of managing auctions and coordinating worker nodes.

## Fault Tolerance
The system is designed to handle faults gracefully. If a worker node fails, the leader can reassign its active auctions to other available workers, ensuring that the auction process continues without interruption. Additionally, if the leader node is killed, a new leader is elected from the remaining nodes, which can then take over the responsibilities of managing the auctions and coordinating the workers.
