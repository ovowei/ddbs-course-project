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

## start mysql server
create a docker
in docker:
apt install mysql-server
vi /etc/mysql/mysql.conf.d/mysqld.cnf

set port to a unused port:
[mysqld]
port = 3307  # example

mkdir -p /var/run/mysqld
chown mysql /var/run/mysqld/
service mysql restart

Authorized remote access:
mysql -u root
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '123456';
FLUSH PRIVILEGES;
exit

mysql -h 127.0.0.1 -P 3307 -u root -p