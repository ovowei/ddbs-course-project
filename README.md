# Distributed Database Systems (DDBS) Course Project

This repository contains the group project for the Distributed Database Systems (DDBS) course at Tsinghua University. The project implements a distributed database system designed to manage both structured and unstructured data. Key features include:

- **Data Partitioning**: Efficiently distribute data across nodes.
- **Replication**: Ensure data availability and fault tolerance.
- **Query Execution**: Support for complex queries over distributed datasets.
- **System Monitoring**: Tools for tracking system performance and health.
- **Integration**: Combines relational database management systems (RDBMSs) with Hadoop HDFS for hybrid data management.

## 1. Base Environment Setup

### MySQL Setup

We use Docker to start the MySQL service for this project. The MySQL version used is 5.7.18. Follow these steps to set up MySQL in Docker:

1. **Pull MySQL Docker Image**:

   ```bash
   docker pull mysql:5.7.18
   ```

2. **Create a Docker Container for MySQL**:

   ```bash
   docker run --name mysql_server -e MYSQL_ROOT_PASSWORD=rootpassword -d mysql:5.7.18
   ```

3. **Start MySQL on Multiple Ports (3310, 3311, 3312)**:

   - Enter the Docker container:

     ```bash
     docker exec -it mysql_server bash
     ```

   - Install MySQL server (if not installed):

     ```bash
     apt update
     apt install mysql-server
     ```

   - Edit the MySQL configuration file:

     ```bash
     vi /etc/mysql/mysql.conf.d/mysqld.cnf
     ```

     Add or modify the port setting. For example:

     ```bash
     [mysqld]
     port = 3310  # For instance 1
     ```

     Repeat this step for ports `3311` and `3312`.

   - Restart MySQL:

     ```bash
     mkdir -p /var/run/mysqld
     chown mysql /var/run/mysqld/
     service mysql restart
     ```

4. **Authorize Remote Access**:

   - Log in to MySQL:

     ```bash
     mysql -u root -p
     ```

   - Update user permissions:

     ```sql
     ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '123456';
     FLUSH PRIVILEGES;
     exit;
     ```

5. **Connect to MySQL on Custom Ports**:

   To connect to MySQL running on ports `3310`, `3311`, or `3312`:

   ```bash
   mysql -h 127.0.0.1 -P 3310 -u root -p
   ```

### HDFS Setup

We use JDK 8 and Hadoop 3.4.1. Follow these steps to set up Hadoop:

1. **Install OpenJDK 8**:

   ```bash
   sudo apt update
   sudo apt install openjdk-8-jdk
   java -version
   ```

2. **Install Hadoop 3.4.1**:

   ```bash
   wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
   tar -xzvf hadoop-3.4.1.tar.gz
   mv hadoop-3.4.1 /home/user/
   ```

3. **Configure Environment**:

   Edit `~/.bashrc` to add Hadoop and Java environment variables:

   ```bash
   # Hadoop Environment Variables
   export PATH=$PATH:/home/user/hadoop-3.4.1/bin:/home/user/hadoop-3.4.1/sbin

   # Java Environment Variables
   export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
   export JRE_HOME=${JAVA_HOME}/jre
   export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
   export PATH=${JAVA_HOME}/bin:$PATH
   ```

   Apply the changes:

   ```bash
   source ~/.bashrc
   ```

4. **Configure Hadoop**:

   Edit Hadoop's `core-site.xml` and `hdfs-site.xml` files based on your requirements. Refer to the official Hadoop documentation or setup guides.

5. **Start HDFS**:

   ```bash
   start-dfs.sh
   ```

   Verify the services:

   ```bash
   jps
   ```

6. **Basic HDFS Commands**:

   ```bash
   hdfs dfs -mkdir input
   hdfs dfs -put test.txt input/
   hdfs dfs -cat input/test.txt
   hdfs dfs -get input/test.txt ./test.txt
   ```

## 2. Project Setup

### Requirements

Install the necessary dependencies:

```bash
sudo apt update
sudo apt install libmysqlcppconn-dev systemctl
sudo systemctl start mysql
```

### Compilation

To compile the project:

```bash
mkdir build
cd build
cmake ..
make -j
```

### Running the Project

Run the compiled binaries:

```bash
cd build
./server
./client
```

## 3. Command Line Usage Instructions

The client supports the following commands:

```shell
Enter a command:
    QUERY <query statement>
    BEREAD
    POPULAR ["daily", "weekly", "monthly"]
    MONITOR
    REGISTER <HOST:port>,<user>,<password>,<schema>
    DUMP <node_num>
    EXIT
Command:
```

### Command Descriptions

1. **QUERY**: Execute an SQL query.

   Example:

   ```bash
   QUERY SELECT * FROM users;
   ```

2. **BEREAD**: Retrieve data related to user or article activity.

3. **POPULAR**: Fetch the top 5 most popular articles for a specified period (`daily`, `weekly`, `monthly`).

   Example:

   ```bash
   POPULAR daily
   ```

4. **MONITOR**: Monitor the database nodes, including connection status and workloads.

5. **REGISTER**: Register a new database node.

   Example:

   ```bash
   REGISTER 127.0.0.1:3312,root,123456,standby1,
   ```

6. **DUMP**: Dump a specific node. If a standby node is ready, it will be promoted to primary.

   Example:

   ```bash
   DUMP 0
   ```

7. **EXIT**: Exit the client session.

## 4. Additional Notes

- Ensure all dependencies are correctly installed before running the project.
- Follow the configuration steps carefully to avoid setup issues.
- Refer to the official documentation for any additional configurations or troubleshooting.

