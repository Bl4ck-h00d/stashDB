# StashDB: distributed K-V store

StashDB is a distributed key-value store server, built in Go, featuring efficient storage and data management. The primary storage engine used is **BoltDB**, offering simplicity and performance for key-value operations. 
StashDB also leverages the **Raft consensus** algorithm for fault tolerance and high availability across nodes in the cluster. It supports both **gRPC** and **RESTful APIs** for easy integration and interaction with your data.

### Key Features:
* Primary Storage Engine: Built on top of BoltDB for efficient, persistent key-value storage.
* Raft Consensus Algorithm: Ensures fault tolerance and strong consistency in a distributed environment.
* gRPC & RESTful APIs: Interface with StashDB using flexible communication protocols.
* Database Replication: Built-in replication to maintain high availability across nodes.
* Cluster Setup: Easily deploy and configure distributed clusters.
* Secure Communication: Supports secure communication between nodes using TLS.


### Benchmarks

#### Total concurrent users: 1000

![alt text](assets/benchmarks.png)