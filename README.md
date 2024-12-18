# Distributed Database Systems (DDBS) Course Project

This repository contains the group project for the Distributed Database Systems (DDBS) course at Tsinghua University. The project implements a distributed database system designed to manage both structured and unstructured data. Key features include:

- **Data Partitioning**: Efficiently distribute data across nodes.
- **Replication**: Ensure data availability and fault tolerance.
- **Query Execution**: Support for complex queries over distributed datasets.
- **System Monitoring**: Tools for tracking system performance and health.
- **Integration**: Combines relational database management systems (RDBMSs) with Hadoop HDFS for hybrid data management.

## Requirements
To set up the project, ensure the following dependencies are installed:

```bash
sudo apt update
sudo apt install libmysqlcppconn-dev systemctl
sudo systemctl start mysql
```

## Compilation
Follow these steps to compile the project:

```bash
mkdir build
cd build
cmake ..
make -j
```

## Running the Project
To run the project, execute the compiled binaries as required:

```bash
./connector_example # tmp
```
