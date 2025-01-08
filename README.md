# StashDB: Distributed Key-Value Store
StashDB is a distributed key-value store server built in Go, offering high performance and flexibility. The primary storage engine is BoltDB, but it also supports LevelDB for users who prefer it. StashDB utilizes the Raft consensus algorithm to ensure fault tolerance and high availability across nodes in the cluster. It also provides easy-to-use gRPC and RESTful APIs for data interaction.

### Key Features:
* **Storage Engines**: Supports both BoltDB and LevelDB for efficient, persistent key-value storage. Choose the storage engine that best fits your needs.
* **Raft Consensus Algorithm**: Ensures fault tolerance and strong consistency in a distributed environment.
* **gRPC & RESTful APIs**: Interface with StashDB using flexible communication protocols.
* **Database Replication**: Built-in replication to maintain high availability across nodes.
* **Cluster Setup**: Easily deploy and configure distributed clusters.
* **Secure Communication**: Supports secure communication between nodes using TLS.
* **Supported Databases**:
  - **BoltDB**: The default storage engine, providing a simple and fast key-value store.
  - **LevelDB**: An alternative storage engine for users who prefer it. You can specify the storage engine during setup.



### Benchmarks

#### Total concurrent users: 1000

![alt text](assets/benchmarks.png)
