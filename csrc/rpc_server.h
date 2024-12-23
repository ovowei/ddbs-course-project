#ifndef RPC_SERVER_H
#define RPC_SERVER_H
/* Standard C++ includes */
#include <bits/stdc++.h>
#include <stdlib.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
/*
  Include directly the different
  headers from cppconn/ and mysql_driver.h + mysql_util.h
  (and mysql_connection.h). This will reduce your build time!
*/
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

#include "mysql_connection.h"

// include the sql parser
#include "SQLParser.h"

// contains printing utilities
#include <condition_variable>
#include <mutex>

#include "expr_tree.h"
#include "util/sqlhelper.h"

typedef long long ll;
class Timer {
   public:
    Timer() = default;

    void begin() { clock_gettime(CLOCK_REALTIME, &start_); }

    uint64_t end(uint64_t loop = 1) {
        this->loop = loop;
        clock_gettime(CLOCK_REALTIME, &end_);
        uint64_t ns_all = (end_.tv_sec - start_.tv_sec) * 1000000000ull + (end_.tv_nsec - start_.tv_nsec);
        duration_ns = ns_all / loop;

        return duration_ns;
    }

    void print() {
        if (duration_ns < 1000) {
            printf("%ldns per loop\n", duration_ns);
        } else {
            printf("%lfus\n", duration_ns * 1.0 / 1000);
        }
    }

    static uint64_t get_time_ns() {
        timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        return 1000000000ull * now.tv_sec + now.tv_nsec;
    }

    static void sleep(uint64_t sleep_ns) {
        Timer clock;

        clock.begin();
        while (true) {
            if (clock.end() >= sleep_ns) {
                return;
            }
        }
    }

    void end_print(uint64_t loop = 1) {
        end(loop);
        print();
    }

   private:
    timespec start_, end_;
    uint64_t loop;
    uint64_t duration_ns;
};

class Table {
   public:
    Schema *schema;
    std::vector<std::vector<std::string>> tuples;
    void print() {
        std::vector<int> max_length;
        for (int i = 0; i < schema->n_columns; i++) {
            int ml = schema->column_name[i].length();
            for (auto it : tuples) {
                ml = std::max(ml, (int)it[i].length());
            }
            max_length.push_back(ml);
        }

        for (int i = 0; i < schema->n_columns; i++) {
            std::cout << "+";
            for (int j = 0; j < max_length[i] + 2; j++) {
                std::cout << "-";
            }
        }
        std::cout << "+" << std::endl;

        for (int i = 0; i < schema->n_columns; i++) {
            std::cout << "| ";
            std::cout << schema->column_name[i];
            for (int j = 0; j < max_length[i] - schema->column_name[i].length() + 1; j++) {
                std::cout << " ";
            }
        }
        std::cout << "|" << std::endl;

        for (int i = 0; i < schema->n_columns; i++) {
            std::cout << "+";
            for (int j = 0; j < max_length[i] + 2; j++) {
                std::cout << "-";
            }
        }
        std::cout << "+" << std::endl;

        for (auto it : tuples) {
            for (int i = 0; i < schema->n_columns; i++) {
                std::cout << "| ";
                std::cout << it[i];
                for (int j = 0; j < max_length[i] - it[i].length() + 1; j++) {
                    std::cout << " ";
                }
            }
            std::cout << "|" << std::endl;
        }

        for (int i = 0; i < schema->n_columns; i++) {
            std::cout << "+";
            for (int j = 0; j < max_length[i] + 2; j++) {
                std::cout << "-";
            }
        }
        std::cout << "+" << std::endl;
    }
    void copy_on_buf(char *buf, const std::string &s) {
        memcpy(buf + buf_offset, s.c_str(), s.length());
        buf_offset += s.length();
    }
    void set_buf(char *buf, size_t &offset) {
        // std::vector<int> max_length;
        // for (int i=0;i<schema->n_columns;i++) {
        //     int ml = schema->column_name[i].length();
        //     for (auto it:tuples) {
        //         ml = std::max(ml, (int)it[i].length());
        //     }
        //     max_length.push_back(ml);
        // }

        // for (auto it : tuples) {
        //     for (int i = 0; i < schema->n_columns; i++) {
        //         memcpy(buf + buf_offset, it[i].c_str(), it[i].length());
        //         buf_offset += it[i].length();
        //     }
        // }
        std::vector<int> max_length;
        for (int i = 0; i < schema->n_columns; i++) {
            int ml = schema->column_name[i].length();
            for (auto it : tuples) {
                ml = std::max(ml, (int)it[i].length());
            }
            max_length.push_back(ml);
        }

        for (int i = 0; i < schema->n_columns; i++) {
            // std::cout<<"+";
            copy_on_buf(buf, "+");
            for (int j = 0; j < max_length[i] + 2; j++) {
                // std::cout<<"-";
                copy_on_buf(buf, "-");
            }
        }
        // std::cout<<"+"<<std::endl;
        copy_on_buf(buf, "+\n");

        for (int i = 0; i < schema->n_columns; i++) {
            // std::cout<<"| ";
            copy_on_buf(buf, "| ");
            // std::cout<<schema->column_name[i];
            copy_on_buf(buf, schema->column_name[i]);
            for (int j = 0; j < max_length[i] - schema->column_name[i].length() + 1; j++) {
                // std::cout<<" ";
                copy_on_buf(buf, " ");
            }
        }
        // std::cout<<"|"<<std::endl;
        copy_on_buf(buf, "|\n");
        for (int i = 0; i < schema->n_columns; i++) {
            // std::cout<<"+";
            copy_on_buf(buf, "+");
            for (int j = 0; j < max_length[i] + 2; j++) {
                // std::cout<<"-";
                copy_on_buf(buf, "-");
            }
        }
        // std::cout<<"+"<<std::endl;
        copy_on_buf(buf, "+\n");
        for (auto it : tuples) {
            for (int i = 0; i < schema->n_columns; i++) {
                // std::cout<<"| ";
                // std::cout<<it[i];
                copy_on_buf(buf, "| ");
                copy_on_buf(buf, it[i]);
                for (int j = 0; j < max_length[i] - it[i].length() + 1; j++) {
                    // std::cout<<" ";
                    copy_on_buf(buf, " ");
                }
            }
            // std::cout<<"|"<<std::endl;
            copy_on_buf(buf, "|\n");
        }

        for (int i = 0; i < schema->n_columns; i++) {
            // std::cout<<"+";
            copy_on_buf(buf, "+\n");
            for (int j = 0; j < max_length[i] + 2; j++) {
                // std::cout<<"-";
                copy_on_buf(buf, "-");
            }
        }
        // std::cout<<"+"<<std::endl;
        copy_on_buf(buf, "+\n");
        offset = buf_offset;
        buf_offset = 0;
    }

   private:
    size_t buf_offset = 0;
};

class DataBaseConnect {
   public:
    sql::Driver *driver;
    sql::Connection *con;
    sql::Connection *thread_local_con;
    sql::ResultSet *res;

    std::string host;
    std::string user;
    std::string password;
    std::string schema;

    enum { DB1, DB2, STANDBY, DUMP } status;

    DataBaseConnect(std::string host, std::string user, std::string password, std::string schema, bool need_to_reset_schema = true)
        : host(host), schema(schema), user(user), password(password) {
        driver = get_driver_instance();
        con = driver->connect(host, user, password);
        thread_local_con = nullptr;
        sql::Statement *stmt = con->createStatement();
        status = STANDBY;
        if (need_to_reset_schema) {
            stmt->execute("DROP DATABASE IF EXISTS " + schema);
            stmt->execute("CREATE DATABASE " + schema);
            con->setSchema(schema);
        } else {
            con->setSchema(schema);
        }
    }

    void execute_batch(std::string sql) {
        std::istringstream ss(sql);
        std::string single_sql;
        sql::Statement *stmt = con->createStatement();

        // 逐条读取 SQL 语句，按分号分割
        while (std::getline(ss, single_sql, ';')) {
            // 去除 SQL 语句前后的空白字符，包括空格、换行符和制表符
            single_sql.erase(0, single_sql.find_first_not_of(" \t\n\r"));
            single_sql.erase(single_sql.find_last_not_of(" \t\n\r") + 1);

            // 如果 SQL 语句非空，执行它
            if (!single_sql.empty()) {
                // printf("Executing: %s\n", single_sql.c_str());
                try {
                    stmt->execute(single_sql);  // 执行 SQL 语句
                } catch (const sql::SQLException &e) {
                    std::cerr << "Error executing SQL: " << e.what() << std::endl;
                }
            }
        }
    }

    void execute(std::string sql) {
        sql::Statement *stmt = con->createStatement();
        stmt->execute(sql);
        // sql::ResultSet *sql_result;
        // while (stmt->getMoreResults()) {
        //     sql_result = stmt->getResultSet();
        // }
        // sql_result->close();
        // stmt->close();
        // delete sql_result;
        // delete stmt;
    }

    sql::ResultSet *executeQuery(std::string sql) {
        sql::Statement *stmt = con->createStatement();
        return stmt->executeQuery(sql);
    }

    // 检查数据库连接状态
    bool check_connection_status() {
        try {
            sql::Statement *stmt = con->createStatement();
            stmt->execute("SELECT 1");
            sql::ResultSet *sql_result;
            while (stmt->getMoreResults()) {
                sql_result = stmt->getResultSet();
            }

            return true;  // 查询成功，返回 true
        } catch (sql::SQLException &e) {
            std::cerr << "Connection failed: " << e.what() << std::endl;
            return false;  // 如果抛出异常，则返回 false
        }
    }

    int get_table_row_count(const std::string &table_name) {
        try {
            sql::Statement *stmt = con->createStatement();
            std::string query = "SELECT COUNT(*) FROM " + table_name;
            sql::ResultSet *res = stmt->executeQuery(query);

            int row_count = 0;
            if (res->next()) {
                row_count = res->getInt(1);  // Return the row count
            }

            delete res;        // Properly delete the ResultSet
            delete stmt;       // Properly delete the Statement
            return row_count;  // Return the row count
        } catch (sql::SQLException &e) {
            std::cerr << "Error checking row count for table '" << table_name << "': " << e.what() << std::endl;
            return 0;  // If an error occurs (e.g., table does not exist), return 0
        }
    }

    int get_current_workload() {
        try {
            std::string query = "SHOW STATUS LIKE 'Threads_connected';";
            sql::Statement *stmt = con->createStatement();
            sql::ResultSet *res = stmt->executeQuery(query);

            int workload = 0;
            if (res->next()) {
                workload = res->getInt(2);  // Get the number of connected threads
            }

            delete res;       // Properly delete the ResultSet
            delete stmt;      // Properly delete the Statement
            return workload;  // Return the current workload (number of threads connected)
        } catch (sql::SQLException &e) {
            std::cerr << "Error fetching current workload: " << e.what() << std::endl;
            return 0;  // If an error occurs, return 0
        }
    }
};

class Control {
   public:
    std::vector<DataBaseConnect *> db_cons;
    std::set<int> standby_dbs;
    DataBaseConnect *primary_db[2];

    Schema *user_schema;
    Schema *article_schema;
    Schema *user_read_schema;
    Schema *be_read_schema;
    Schema *popular_rank_schema;

    std::thread monitor_thread;
    std::mutex mtx;
    std::condition_variable cv;
    bool sufficient_primary = false;

    int popular_rank_id;

    // Function to manually dump a primary database
    void manual_dump_primary(int db_index) {
        if (db_index < 0 || db_index >= 2) {
            std::cerr << "Invalid database index. Please provide 0 or 1." << std::endl;
            return;
        }

        // Set the selected primary database to DUMP
        if (primary_db[db_index]->status == DataBaseConnect::DB1 || primary_db[db_index]->status == DataBaseConnect::DB2) {
            primary_db[db_index]->status = DataBaseConnect::DUMP;
            std::cout << "Primary database " << db_index << " has been dumped." << std::endl;
        } else {
            std::cout << "Database " << db_index << " is already in DUMP state." << std::endl;
        }
        check_and_promote_primary();
    }

    void check_and_promote_primary() {
        int primary_count = 0;
        for (int i = 0; i < 2; ++i) {
            if (primary_db[i]->status == DataBaseConnect::DB1 || primary_db[i]->status == DataBaseConnect::DB2) {
                primary_count++;
            }
        }

        if (primary_count < 2) {
            std::cout << "One or more primary databases are down. Attempting to promote standby DB..." << std::endl;

            // Check if standby DBs are available
            if (!standby_dbs.empty()) {
                int standby_index = *standby_dbs.begin();
                standby_dbs.erase(standby_index);

                // Promote the standby DB to primary
                primary_db[primary_count] = db_cons[standby_index];
                primary_db[primary_count]->status = (primary_count == 0) ? DataBaseConnect::DB1 : DataBaseConnect::DB2;
                std::cout << "Promoted standby DB at index " << standby_index << " to primary DB " << primary_count << "." << std::endl;

                // Download binlog from HDFS and apply it
                std::string binlog_path = "/path/to/binlogs/mysql-bin.000001";  // HDFS path to the binlog
                std::string local_binlog_path = "/tmp/mysql-bin.000001";        // Local path to store the binlog

                // Download binlog from HDFS
                // fs_engine.download_binlog_from_hdfs(binlog_path, local_binlog_path);

                // Apply binlog to new primary
                apply_binlog_to_primary(standby_index, local_binlog_path);

                // Update primary count
                primary_count++;
            } else {
                std::cerr << "No standby databases available to promote." << std::endl;
            }
        } else {
            std::cout << "Both primary databases are running fine." << std::endl;
        }
    }

    void apply_binlog_to_primary(int standby_index, const std::string &binlog_path) {
        // Simulate applying binlog events to the new primary DB (standby)
        std::cout << "Applying binlog events to the new primary DB from " << binlog_path << "..." << std::endl;

        try {
            // Here, you would typically use mysqlbinlog or similar tools to apply the binlog
            // In this case, we assume that the binlog file is available and ready to be applied
            std::string command = "mysqlbinlog " + binlog_path + " | mysql -u root -p";
            int ret = system(command.c_str());
            if (ret != 0) {
                std::cerr << "Error applying binlog: " << ret << std::endl;
            } else {
                std::cout << "Successfully applied binlog to the new primary DB." << std::endl;
            }
        } catch (const std::exception &e) {
            std::cerr << "Error applying binlog: " << e.what() << std::endl;
        }
    }

    // Function to get the monitoring info from all databases
    void get_monitoring_info() {
        std::cout << "----------------------------------------" << std::endl;
        for (auto &db : db_cons) {
            std::cout << "Monitoring info for DB at host: " << db->host << std::endl;

            // Connection status
            // bool connection_status = db->check_connection_status();
            // std::cout << "Connection status: " << (connection_status ? "Connected" : "Disconnected") << std::endl;

            // Output database status (Primary, Standby, Offline)
            std::string status_str;
            switch (db->status) {
                case DataBaseConnect::DB1:
                    status_str = "Primary-DB1";
                    break;
                case DataBaseConnect::DB2:
                    status_str = "Primary-DB2";
                    break;
                case DataBaseConnect::STANDBY:
                    status_str = "Standby";
                    break;
                case DataBaseConnect::DUMP:
                    status_str = "Offline";
                    break;
            }
            std::cout << "Database status: " << status_str << std::endl;

            // List of tables to check
            std::vector<std::string> tables = {"user", "article", "user_read", "be_read", "popular_rank"};

            // Output the row count for each table
            for (const auto &table : tables) {
                int row_count = db->get_table_row_count(table);
                std::cout << "Row count in '" << table << "' table: " << row_count << std::endl;
            }

            // Workload (active threads or queries)
            int workload = db->get_current_workload();
            std::cout << "Current workload (active threads): " << workload << std::endl;

            std::cout << "----------------------------------------" << std::endl;
        }
    }

    // Function to get the monitoring info from all databases
    void get_monitoring_info(char *buf, size_t &offset) {
        // Separator between database info
        snprintf(buf + offset, 1024, "----------------------------------------\n");
        offset += strlen(buf + offset);  // Update offset

        for (auto &db : db_cons) {
            // Append database host info
            snprintf(buf + offset, 1024, "Monitoring info for DB at host: %s\n", db->host.c_str());
            offset += strlen(buf + offset);  // Update offset after writing to buffer

            // Connection status
            bool connection_status = db->check_connection_status();
            snprintf(buf + offset, 1024, "Connection status: %s\n", connection_status ? "Connected" : "Disconnected");
            offset += strlen(buf + offset);  // Update offset

            // Output database status (Primary, Standby, Offline)
            std::string status_str;
            switch (db->status) {
                case DataBaseConnect::DB1:
                    status_str = "Primary-DB1";
                    break;
                case DataBaseConnect::DB2:
                    status_str = "Primary-DB2";
                    break;
                case DataBaseConnect::STANDBY:
                    status_str = "Standby";
                    break;
                case DataBaseConnect::DUMP:
                    status_str = "Offline";
                    break;
            }
            snprintf(buf + offset, 1024, "Database status: %s\n", status_str.c_str());
            offset += strlen(buf + offset);  // Update offset

            // List of tables to check
            std::vector<std::string> tables = {"user", "article", "user_read", "be_read", "popular_rank"};

            // Output the row count for each table
            for (const auto &table : tables) {
                int row_count = db->get_table_row_count(table);
                snprintf(buf + offset, 1024, "Row count in '%s' table: %d\n", table.c_str(), row_count);
                offset += strlen(buf + offset);  // Update offset
            }

            // Workload (active threads or queries)
            int workload = db->get_current_workload();
            snprintf(buf + offset, 1024, "Current workload (active threads): %d\n", workload);
            offset += strlen(buf + offset);  // Update offset

            // Separator between database info
            snprintf(buf + offset, 1024, "----------------------------------------\n");
            offset += strlen(buf + offset);  // Update offset
        }
    }

    void init_schema_user() {
        user_schema = new Schema();
        user_schema->n_columns = 14;
        user_schema->column_name.push_back("timestamp");
        user_schema->column_name.push_back("id");
        user_schema->column_name.push_back("uid");
        user_schema->column_name.push_back("name");
        user_schema->column_name.push_back("gender");
        user_schema->column_name.push_back("email");
        user_schema->column_name.push_back("phone");
        user_schema->column_name.push_back("dept");
        user_schema->column_name.push_back("grade");
        user_schema->column_name.push_back("language");
        user_schema->column_name.push_back("region");
        user_schema->column_name.push_back("role");
        user_schema->column_name.push_back("preferTags");
        user_schema->column_name.push_back("obtainedCredits");
    }

    void init_schema_article() {
        article_schema = new Schema();
        article_schema->n_columns = 12;
        article_schema->column_name.push_back("timestamp");
        article_schema->column_name.push_back("id");
        article_schema->column_name.push_back("aid");
        article_schema->column_name.push_back("title");
        article_schema->column_name.push_back("category");
        article_schema->column_name.push_back("abstract");
        article_schema->column_name.push_back("articleTags");
        article_schema->column_name.push_back("authors");
        article_schema->column_name.push_back("language");
        article_schema->column_name.push_back("text");
        article_schema->column_name.push_back("image");
        article_schema->column_name.push_back("video");
    }

    void init_schema_user_read() {
        user_read_schema = new Schema();
        user_read_schema->n_columns = 9;
        user_read_schema->column_name.push_back("timestamp");
        user_read_schema->column_name.push_back("id");
        user_read_schema->column_name.push_back("uid");
        user_read_schema->column_name.push_back("aid");
        user_read_schema->column_name.push_back("readTimeLength");
        user_read_schema->column_name.push_back("agreeOrNot");
        user_read_schema->column_name.push_back("commentOrNot");
        user_read_schema->column_name.push_back("shareOrNot");
        user_read_schema->column_name.push_back("commentDetail");
    }

    void init_schema_be_read() {
        be_read_schema = new Schema();
        be_read_schema->n_columns = 11;
        be_read_schema->column_name.push_back("timestamp");
        be_read_schema->column_name.push_back("id");
        be_read_schema->column_name.push_back("aid");
        be_read_schema->column_name.push_back("readNum");
        be_read_schema->column_name.push_back("readUidList");
        be_read_schema->column_name.push_back("commentNum");
        be_read_schema->column_name.push_back("commentUidList");
        be_read_schema->column_name.push_back("agreeNum");
        be_read_schema->column_name.push_back("agreeUidList");
        be_read_schema->column_name.push_back("shareNum");
        be_read_schema->column_name.push_back("shareUidList");
    }

    void init_schema_popular_rank() {
        popular_rank_schema = new Schema();
        popular_rank_schema->n_columns = 4;
        popular_rank_schema->column_name.push_back("timestamp");
        popular_rank_schema->column_name.push_back("id");
        popular_rank_schema->column_name.push_back("temporalGranularity");
        popular_rank_schema->column_name.push_back("articleAidList");
    }

    void create_popular_rank() {
        popular_rank_id = 0;
        std::string create_sql1 = "DROP TABLE IF EXISTS `popular_rank`";
        std::string create_sql2 =
            " \
            CREATE TABLE `popular_rank` ( \
            `timestamp` char(14) DEFAULT NULL, \
            `id` char(7) DEFAULT NULL, \
            `temporalGranularity` char(10) DEFAULT NULL, \
            `articleAidList` char(64) DEFAULT NULL \
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8; \
        ";

        primary_db[0]->execute(create_sql1);
        primary_db[0]->execute(create_sql2);
        primary_db[1]->execute(create_sql1);
        primary_db[1]->execute(create_sql2);
    }

    int add_standby_db(const std::string &host, const std::string &user, const std::string &password, const std::string &schema) {
        try {
            // 创建一个新的数据库连接
            DataBaseConnect *new_db = new DataBaseConnect(host, user, password, schema, true);

            // 检查连接是否成功
            sql::Statement *stmt = new_db->con->createStatement();
            sql::ResultSet *res = stmt->executeQuery("SELECT 1;");
            delete res;

            // 设置状态为 STANDBY
            new_db->status = DataBaseConnect::STANDBY;

            // 添加到 Control 的管理中
            db_cons.push_back(new_db);
            standby_dbs.insert(db_cons.size() - 1);  // 记录为 standby

            std::cout << "Successfully added a new standby database." << std::endl;
            return 1;
        } catch (sql::SQLException &e) {
            std::cerr << "Error adding standby DB: " << e.what() << "\n";
            return 0;
        }
    }

    void monitor_and_manage_dbs() {
        while (true) {
            bool primary_down = false;
            int primary_count = 0;

            for (size_t i = 0; i < db_cons.size(); ++i) {
                try {
                    if (db_cons[i]->thread_local_con == nullptr) {
                        db_cons[i]->thread_local_con = db_cons[i]->driver->connect(db_cons[i]->host, db_cons[i]->user, db_cons[i]->password);

                        sql::Statement *stmt = db_cons[i]->thread_local_con->createStatement();

                        stmt->execute("DROP DATABASE IF EXISTS test");
                        stmt->execute("CREATE DATABASE test");
                        db_cons[i]->thread_local_con->setSchema("test");
                    }
                    sql::Statement *stmt = db_cons[i]->thread_local_con->createStatement();
                    sql::ResultSet *res = stmt->executeQuery("SELECT 1;");
                    delete res;
                    if (db_cons[i]->status == DataBaseConnect::DB1 || db_cons[i]->status == DataBaseConnect::DB2) {
                        primary_count++;
                    }
                } catch (sql::SQLException &e) {
                    if (db_cons[i]->status == DataBaseConnect::DB1 || db_cons[i]->status == DataBaseConnect::DB2) {
                        primary_down = true;
                        db_cons[i]->status = DataBaseConnect::DUMP;
                    } else if (db_cons[i]->status == DataBaseConnect::STANDBY) {
                        db_cons[i]->status = DataBaseConnect::DUMP;
                        standby_dbs.erase(i);
                    }
                    printf("Database at host %s is down. Error: %s\n", db_cons[i]->host.c_str(), e.what());
                }
            }

            if (primary_count < 2) {
                std::unique_lock<std::mutex> lock(mtx);
                sufficient_primary = false;
                cv.notify_all();  // 通知主线程
                std::cerr << "Insufficient primary databases. Blocking until at least 2 primary databases are available." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(10));
                continue;
            }

            std::unique_lock<std::mutex> lock(mtx);
            sufficient_primary = true;
            cv.notify_all();  // 通知主线程继续执行

            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }

    void wait_for_primary() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this] { return sufficient_primary; });  // 等待 sufficient_primary 为 true
        std::cout << "Sufficient primary databases available. Resuming execution." << std::endl;
    }

    Control(const std::string &primary1_host, const std::string &primary1_user, const std::string &primary1_password, const std::string &primary1_schema,
            const std::string &primary2_host, const std::string &primary2_user, const std::string &primary2_password, const std::string &primary2_schema) {
        // initialize schema
        init_schema_user();
        init_schema_article();
        init_schema_user_read();
        init_schema_be_read();
        init_schema_popular_rank();

        try {
            // Initialize first primary database
            primary_db[0] = new DataBaseConnect(primary1_host, primary1_user, primary1_password, primary1_schema, true);
            primary_db[0]->status = DataBaseConnect::DB1;
            db_cons.push_back(primary_db[0]);

            // Initialize second primary database
            primary_db[1] = new DataBaseConnect(primary2_host, primary2_user, primary2_password, primary2_schema, true);
            primary_db[1]->status = DataBaseConnect::DB2;
            db_cons.push_back(primary_db[1]);

            create_popular_rank();

            std::cout << "Two primary databases initialized successfully." << std::endl;
        } catch (sql::SQLException &e) {
            std::cerr << "Error initializing primary databases: " << e.what() << "\n";
            throw;
        }

        // Start monitoring in a separate thread
        monitor_thread = std::thread(&Control::monitor_and_manage_dbs, this);
    }
    ~Control() {
        if (monitor_thread.joinable()) {
            monitor_thread.join();
        }
    }

    void bulk_load(int dbms_no) {
        wait_for_primary();
        Timer timer;

        if (dbms_no == 1) {
            const std::vector<std::string> sql_files = {"../sql/user.sql",       "../sql/user_bj.sql",   "../sql/article.sql",
                                                        "../sql/article_tc.sql", "../sql/user_read.sql", "../sql/user_read_bj.sql"};

            // const std::vector<std::string> sql_files = {"../sql/user.sql"};

            for (const auto &file_path : sql_files) {
                try {
                    std::ifstream file(file_path);
                    if (!file.is_open()) {
                        std::cerr << "Failed to open file: " << file_path << "\n";
                        continue;
                    }

                    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
                    timer.begin();
                    primary_db[0]->execute_batch(sql);
                    timer.end_print();

                    std::cout << "Executed SQL from file: " << file_path << "\n";
                } catch (const std::exception &e) {
                    std::cerr << "Error executing SQL from file " << file_path << ": " << e.what() << "\n";
                }
            }

            puts("Bulk loaded user, article, read table for DB1!");
        }
        if (dbms_no == 2) {
            const std::vector<std::string> sql_files = {"../sql/user.sql", "../sql/user_hk.sql", "../sql/article_all.sql", "../sql/user_read.sql",
                                                        "../sql/user_read_hk.sql"};

            for (const auto &file_path : sql_files) {
                try {
                    std::ifstream file(file_path);
                    if (!file.is_open()) {
                        std::cerr << "Failed to open file: " << file_path << "\n";
                        continue;
                    }

                    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
                    timer.begin();
                    primary_db[1]->execute_batch(sql);
                    timer.end_print();

                    std::cout << "Executed SQL from file: " << file_path << "\n";
                } catch (const std::exception &e) {
                    std::cerr << "Error executing SQL from file " << file_path << ": " << e.what() << "\n";
                }
            }

            puts("Bulk loaded user, article, read table for DB2!");
        }
        wait_for_primary();
    }

    void populate_be_read() {
        Timer timer;
        timer.begin();
        // here the timestamps are from user_read table
        // the aggregated timestamp of be_read table should be the maximum value of these timestamps (the latest result)
        std::string query =
            " \
            select \"timestamp\", aid, uid, commentOrNot, agreeOrNot, shareOrNot \
            from user_read join user on uid = uid join article on aid = aid \
        ";
        Table *query_res = process_query(query);
        // query_res->print();

        // generate data
        std::vector<ll> timestamp;
        std::vector<int> id;
        std::vector<std::string> aid;
        std::vector<int> readNum;
        std::vector<std::string> readUidList;
        std::vector<int> commentNum;
        std::vector<std::string> commentUidList;
        std::vector<int> agreeNum;
        std::vector<std::string> agreeUidList;
        std::vector<int> shareNum;
        std::vector<std::string> shareUidList;

        int n_rows = 0;
        std::unordered_map<std::string, int> hashmap;
        for (auto it : query_res->tuples) {
            std::string cur_aid = it[1];
            int row_id;
            if (hashmap.find(cur_aid) == hashmap.end()) {
                row_id = hashmap[cur_aid] = n_rows++;
                timestamp.push_back(0);
                id.push_back(row_id);
                aid.push_back(cur_aid);
                readNum.push_back(0);
                readUidList.push_back("");
                commentNum.push_back(0);
                commentUidList.push_back("");
                agreeNum.push_back(0);
                agreeUidList.push_back("");
                shareNum.push_back(0);
                shareUidList.push_back("");
            } else {
                row_id = hashmap[cur_aid];
            }
            timestamp[row_id] = std::max(timestamp[row_id], atoll(it[0].c_str()));
            readNum[row_id]++;
            readUidList[row_id] += it[2] + ", ";
            if (it[3] == "1") {
                commentNum[row_id]++;
                commentUidList[row_id] += it[2] + ", ";
            }
            if (it[4] == "1") {
                agreeNum[row_id]++;
                agreeUidList[row_id] += it[2] + ", ";
            }
            if (it[5] == "1") {
                shareNum[row_id]++;
                shareUidList[row_id] += it[2] + ", ";
            }
        }
        for (int i = 0; i < n_rows; i++) {  // clean extra comma and space
            readUidList[i].pop_back();
            readUidList[i].pop_back();
            if (commentNum[i] != 0) {
                commentUidList[i].pop_back();
                commentUidList[i].pop_back();
            }
            if (agreeNum[i] != 0) {
                agreeUidList[i].pop_back();
                agreeUidList[i].pop_back();
            }
            if (shareNum[i] != 0) {
                shareUidList[i].pop_back();
                shareUidList[i].pop_back();
            }
        }

        // create be_read_tmp(unpartitioned be_read) table
        std::string create_sql1 = "DROP TABLE IF EXISTS `be_read_tmp`;";
        std::string create_sql2 =
            " \
            CREATE TABLE `be_read_tmp` ( \
            `timestamp` char(14) DEFAULT NULL, \
            `id` char(7) DEFAULT NULL, \
            `aid` char(7) DEFAULT NULL, \
            `readNum` char(7) DEFAULT NULL, \
            `readUidList` TEXT DEFAULT NULL, \
            `commentNum` char(7) DEFAULT NULL, \
            `commentUidList` TEXT DEFAULT NULL, \
            `agreeNum` char(7) DEFAULT NULL, \
            `agreeUidList` TEXT DEFAULT NULL, \
            `shareNum` char(7) DEFAULT NULL, \
            `shareUidList` TEXT DEFAULT NULL \
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8; \
        ";

        // 执行创建表的 SQL
        primary_db[0]->execute(create_sql1);
        primary_db[0]->execute(create_sql2);
        primary_db[1]->execute(create_sql1);
        primary_db[1]->execute(create_sql2);

        // populate be_read_tmp(unpartitioned be_read) table
        std::string insert_sql1 = "LOCK TABLES `be_read_tmp` WRITE;";
        std::string insert_sql2 = "INSERT INTO `be_read_tmp` VALUES ";
        for (int i = 0; i < n_rows; i++) {
            insert_sql2 += "(\"";
            insert_sql2 += std::to_string(timestamp[i]) + "\", \"";
            insert_sql2 += std::to_string(id[i]) + "\", \"";
            insert_sql2 += aid[i] + "\", \"";
            insert_sql2 += std::to_string(readNum[i]) + "\", \"";
            insert_sql2 += readUidList[i] + "\", \"";
            insert_sql2 += std::to_string(commentNum[i]) + "\", \"";
            insert_sql2 += commentUidList[i] + "\", \"";
            insert_sql2 += std::to_string(agreeNum[i]) + "\", \"";
            insert_sql2 += agreeUidList[i] + "\", \"";
            insert_sql2 += std::to_string(shareNum[i]) + "\", \"";
            insert_sql2 += shareUidList[i] + "\")";
            if (i != n_rows - 1) {
                insert_sql2 += ", ";
            } else {
                insert_sql2 += ";";
            }
        }
        std::string insert_sql3 = "UNLOCK TABLES";

        // 使用 DataBaseConnect 的 execute 方法来执行 SQL
        primary_db[0]->execute(insert_sql1);
        primary_db[0]->execute(insert_sql2);
        primary_db[0]->execute(insert_sql3);

        primary_db[1]->execute(insert_sql1);
        primary_db[1]->execute(insert_sql2);
        primary_db[1]->execute(insert_sql3);

        // partition
        std::string partition_sql1 = "DROP TABLE IF EXISTS `be_read`;";
        std::string partition_sql2_1 = "CREATE TABLE `be_read` SELECT * FROM be_read_tmp WHERE aid in (SELECT aid FROM article);";
        std::string partition_sql2_2 = "CREATE TABLE `be_read` SELECT * FROM be_read_tmp;";

        // 使用 DataBaseConnect 的 execute 方法来执行 SQL
        primary_db[0]->execute(partition_sql1);
        primary_db[0]->execute(partition_sql2_1);
        primary_db[1]->execute(partition_sql1);
        primary_db[1]->execute(partition_sql2_2);

        timer.end_print();
        puts("Populated be_read table!");
    }

    void populate_popular_rank(ll query_time, std::string query_granularity, std::vector<std::string> &aid, std::vector<std::string> &text,
                               std::vector<std::string> &image, std::vector<std::string> &video) {
        Timer timer;
        timer.begin();

        if (query_time < 1506000000000ll || query_time > 1506000009995ll) {
            printf("Timestamp Should Between 1506000000000 and 1506000009995!\n");
            return;
        }
        if (query_granularity != "daily" && query_granularity != "weekly" && query_granularity != "monthly") {
            printf("TemporalGranularity Should Be \"daily\"/\"weekly\"/\"monthly\" ");
            return;
        }

        // here the timestamps are from article table
        std::string query =
            " \
            select \"timestamp\", aid, readNum, \"text\", image, video \
            from article join be_read on aid = aid \
        ";
        Table *query_res = process_query(query);

        // get top 5
        ll timestamp_min = query_time;
        ll timestamp_max;
        if (query_granularity == "daily") {
            timestamp_max = query_time + 24;
        } else if (query_granularity == "weekly") {
            timestamp_max = query_time + 24 * 7;
        } else if (query_granularity == "monthly") {
            timestamp_max = query_time + 24 * 30;
        }
        std::vector<std::pair<int, int>> buff;
        for (int i = 0; i < query_res->tuples.size(); i++) {
            ll timestamp = atoll(query_res->tuples[i][0].c_str());
            int readNum = atoi(query_res->tuples[i][2].c_str());
            if (timestamp < timestamp_min || timestamp > timestamp_max) {
                continue;
            }
            buff.push_back({readNum, i});
        }
        std::sort(buff.begin(), buff.end(), std::greater<std::pair<int, int>>());

        // insert into popular_rank table
        std::string insert_sql1 = "LOCK TABLES `popular_rank` WRITE;";
        std::string insert_sql2 = "INSERT INTO `popular_rank` VALUES (\"";
        insert_sql2 += std::to_string(query_time) + "\", \"";
        insert_sql2 += "p" + std::to_string(popular_rank_id++) + "\", \"";
        insert_sql2 += query_granularity + "\", \"";
        for (int i = 0; i < 5; i++) {
            insert_sql2 += query_res->tuples[buff[i].second][1];
            if (i != 4) {
                insert_sql2 += ", ";
            }
        }
        insert_sql2 += "\")";
        std::string insert_sql3 = "UNLOCK TABLES";

        // 假设 primary_db[0] 和 primary_db[1] 已经被初始化并连接到数据库
        if (query_granularity == "daily") {
            // 使用 primary_db[0] 执行 SQL
            primary_db[0]->execute(insert_sql1);
            primary_db[0]->execute(insert_sql2);
            primary_db[0]->execute(insert_sql3);
        } else {
            // 使用 primary_db[1] 执行 SQL
            primary_db[1]->execute(insert_sql1);
            primary_db[1]->execute(insert_sql2);
            primary_db[1]->execute(insert_sql3);
        }

        // return details to client
        for (int i = 0; i < 5; i++) {
            aid.push_back(query_res->tuples[buff[i].second][1]);
            text.push_back(query_res->tuples[buff[i].second][3]);
            image.push_back(query_res->tuples[buff[i].second][4]);
            video.push_back(query_res->tuples[buff[i].second][5]);
        }

        timer.end_print();
        puts("Populated popular_rank table!");

        return;
    }
    // convert from sql::ResultSet to our Table structure
    void convert_to_table(sql::ResultSet *res, Table *table) {
        int n_columns = table->schema->n_columns;
        while (res->next()) {
            std::vector<std::string> tuple;
            for (int i = 0; i < n_columns; i++) {
                tuple.push_back(res->getString(i + 1));
            }
            table->tuples.push_back(tuple);
        }
    }

    // Recursive check if condition holds for a given tuple
    bool check_condition(std::shared_ptr<Expr> condition, Schema *schema, const std::vector<std::string> &tuple) {
        // Handle AND/OR expressions
        if (condition->type == Expr::AND) {
            return check_condition(condition->child1, schema, tuple) && check_condition(condition->child2, schema, tuple);
        }
        if (condition->type == Expr::OR) {
            return check_condition(condition->child1, schema, tuple) || check_condition(condition->child2, schema, tuple);
        }

        // For comparison expressions (EQUALS, NOTEQUALS)
        std::string str1 = get_column_value(condition->child1, schema, tuple);
        std::string str2 = get_column_value(condition->child2, schema, tuple);

        // Check specific conditions
        switch (condition->type) {
            case Expr::EQUALS:
                return str1 == str2;
            case Expr::NOTEQUALS:
                return str1 != str2;
            default:
                return false;
        }
    }

    // Helper function to extract column value (either constant or from tuple)
    std::string get_column_value(std::shared_ptr<Expr> expr, Schema *schema, const std::vector<std::string> &tuple) {
        if (expr->type == Expr::CONSTVALUE) {
            return expr->const_string;  // If it's a constant, return it
        }

        // Otherwise, search for the column name in the schema and return the corresponding value in the tuple
        for (int i = 0; i < schema->n_columns; ++i) {
            if (schema->column_name[i] == expr->column_name) {
                return tuple[i];
            }
        }
        return "";  // In case the column name does not exist, return an empty string
    }

    // (recursively) execute the query based on the plan tree
    Table *execute_node(std::shared_ptr<Treenode> node) {
        if (node == nullptr) {
            return nullptr;
        }

        // subquery on mysql1
        if (node->position == node->MYSQL1) {
            sql::ResultSet *res = primary_db[0]->executeQuery(node->subquery_sql);
            Table *t = new Table();
            t->schema = node->schema;
            convert_to_table(res, t);
            return t;
        }

        // subquery on mysql2
        if (node->position == node->MYSQL2) {
            sql::ResultSet *res = primary_db[1]->executeQuery(node->subquery_sql);
            Table *t = new Table();
            t->schema = node->schema;
            convert_to_table(res, t);
            return t;
        }

        // compute on control layer

        Table *table1 = execute_node(node->child1);
        Table *table2 = execute_node(node->child2);

        // join
        if (node->operation == node->JOIN) {
            // for simplicity, assume join always has one key
            std::string key1 = node->joinDescription->key1;
            int key_id_table1 = -1;
            for (int i = 0; i < node->schema->n_columns; i++) {
                if (table1->schema->column_name[i] == key1) {
                    key_id_table1 = i;
                    break;
                }
            }
            if (key_id_table1 == -1) {
                printf("Join Key Not Found in Table1!\n");
                return nullptr;
            }
            std::string key2 = node->joinDescription->key2;
            int key_id_table2 = -1;
            for (int i = 0; i < node->schema->n_columns; i++) {
                if (table2->schema->column_name[i] == key2) {
                    key_id_table2 = i;
                    break;
                }
            }
            if (key_id_table2 == -1) {
                printf("Join Key Not Found in Table2!\n");
                return nullptr;
            }

            // data source of each column, in form of pair{table_id, column_id}
            std::vector<std::pair<int, int>> source;
            for (int i = 0; i < node->schema->n_columns; i++) {
                bool found = false;
                std::string column = node->schema->column_name[i];
                for (int j = 0; j < table1->schema->n_columns; j++) {
                    if (column == table1->schema->column_name[j]) {
                        source.push_back({1, j});
                        found = true;
                        break;
                    }
                }
                if (found) continue;
                for (int j = 0; j < table2->schema->n_columns; j++) {
                    if (column == table2->schema->column_name[j]) {
                        source.push_back({2, j});
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    printf("Join Column Not Found in Both Tables!\n");
                    return nullptr;
                }
            }

            // use hashmap to join
            Table *res = new Table();
            res->schema = node->schema;
            std::unordered_map<std::string, std::vector<int>> hashmap;
            // use table1 to build
            for (int i = 0; i < table1->tuples.size(); i++) {
                std::string key = table1->tuples[i][key_id_table1];
                if (hashmap.find(key) == hashmap.end()) {
                    std::vector<int> tmp;
                    hashmap.insert({key, tmp});
                }
                hashmap[key].push_back(i);
            }
            // use table2 to probe
            for (int i = 0; i < table2->tuples.size(); i++) {
                std::string key = table2->tuples[i][key_id_table2];
                if (hashmap.find(key) != hashmap.end()) {
                    for (int table1_row_id : hashmap[key]) {
                        int table2_row_id = i;
                        std::vector<std::string> tuple;
                        for (int j = 0; j < res->schema->n_columns; j++) {
                            int table_id = source[j].first;
                            int column_id = source[j].second;
                            if (table_id == 1) {
                                tuple.push_back(table1->tuples[table1_row_id][column_id]);
                            } else {
                                tuple.push_back(table2->tuples[table2_row_id][column_id]);
                            }
                        }
                        res->tuples.push_back(tuple);
                    }
                }
            }
            return res;
        }

        // union
        if (node->operation == node->UNION) {
            // for simplicity, assume union always has one key
            std::string key = node->unionDescription->key1;
            int key_id = -1;
            for (int i = 0; i < node->schema->n_columns; i++) {
                if (node->schema->column_name[i] == key) {
                    key_id = i;
                    break;
                }
            }
            if (key_id == -1) {
                printf("Union Key Not Found!\n");
                return nullptr;
            }

            // use hashmap to check duplication
            Table *res = new Table();
            res->schema = node->schema;
            std::unordered_map<std::string, int> hashmap;
            for (auto it : table1->tuples) {
                res->tuples.push_back(it);
                std::string key = it[key_id];
                hashmap.insert({key, 1});
            }
            for (auto it : table2->tuples) {
                std::string key = it[key_id];
                if (hashmap.find(key) == hashmap.end()) {
                    res->tuples.push_back(it);
                }
            }
            return res;
        }

        // groupby
        if (node->operation == node->GROUPBY) {
            return nullptr;
        }

        // orderby
        if (node->operation == node->ORDERBY) {
            return nullptr;
        }

        // limit
        if (node->operation == node->LIMIT) {
            int limit_num = node->limitDescription->limit_num;
            int num = std::min(limit_num, (int)table1->tuples.size());
            Table *res = new Table();
            res->schema = node->schema;
            for (int i = 0; i < num; i++) {
                res->tuples.push_back(table1->tuples[i]);
            }
            return res;
        }

        // filter
        if (node->operation == node->FILTER) {
            std::shared_ptr<Expr> condition = node->filterDescription->condition;

            Table *res = new Table();
            res->schema = node->schema;
            for (auto it : table1->tuples) {
                if (check_condition(condition, res->schema, it)) {
                    res->tuples.push_back(it);
                }
            }
            return res;
        }

        // project
        if (node->operation == node->PROJECT) {
            // data source of each colomn
            std::vector<int> source;
            for (int i = 0; i < node->schema->n_columns; i++) {
                bool found = false;
                std::string column = node->schema->column_name[i];
                for (int j = 0; j < table1->schema->n_columns; j++) {
                    if (column == table1->schema->column_name[j]) {
                        source.push_back(j);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    printf("Project Column Not Found!\n");
                    return nullptr;
                }
            }

            Table *res = new Table();
            res->schema = node->schema;
            for (auto it : table1->tuples) {
                std::vector<std::string> tuple;
                for (int i = 0; i < res->schema->n_columns; i++) {
                    tuple.push_back(it[source[i]]);
                }
                res->tuples.push_back(tuple);
            }
            return res;
        }

        return nullptr;
    }

    std::shared_ptr<Treenode> build_plan_tree_table(hsql::TableRef *table) {
        Schema *schema;
        std::string name = table->name;

        if (name == "user") {
            schema = user_schema;
        } else if (name == "article") {
            schema = article_schema;
        } else if (name == "user_read") {
            schema = user_read_schema;
        } else if (name == "be_read") {
            schema = be_read_schema;
        } else if (name == "popular_rank") {
            schema = popular_rank_schema;
        } else {
            printf("Unknown Table!\n");
            return nullptr;
        }

        // 获取每个 shard 并做 union 操作
        std::shared_ptr<Treenode> child1 = std::make_shared<Treenode>(Treenode::Position::MYSQL1, schema);
        child1->subquery_sql = "select * from " + name;

        std::shared_ptr<Treenode> child2 = std::make_shared<Treenode>(Treenode::Position::MYSQL2, schema);
        child2->subquery_sql = "select * from " + name;

        // 创建 UNION 节点
        std::shared_ptr<Treenode> ret = std::make_shared<Treenode>(Treenode::Operation::UNION, child1, child2);
        ret->schema = schema;
        ret->unionDescription = std::make_shared<KeyDescription>();
        ret->unionDescription->key1 = "id";  // 假设 Union 是基于 "id" 键

        return ret;
    }

    // Join two tables on control node
    std::shared_ptr<Treenode> build_plan_tree_join(const hsql::JoinDefinition *join) {
        // Get two tables
        std::shared_ptr<Treenode> child1;
        if (join->left->type == hsql::kTableSelect) {
            child1 = build_plan_tree_select(join->left->select);
        } else if (join->left->type == hsql::kTableJoin) {
            child1 = build_plan_tree_join(join->left->join);
        } else if (join->left->type == hsql::kTableName) {
            child1 = build_plan_tree_table(join->left);
        } else {
            printf("Unsupported Syntax!\n");
            return nullptr;
        }

        std::shared_ptr<Treenode> child2;
        if (join->right->type == hsql::kTableSelect) {
            child2 = build_plan_tree_select(join->right->select);
        } else if (join->right->type == hsql::kTableJoin) {
            child2 = build_plan_tree_join(join->right->join);
        } else if (join->right->type == hsql::kTableName) {
            child2 = build_plan_tree_table(join->right);
        } else {
            printf("Unsupported Syntax!\n");
            return nullptr;
        }
        // Make sure that join condition is a equal expression
        if (join->condition->type != hsql::kExprOperator) {
            printf("Unsupported Syntax!\n");
            return nullptr;
        }
        if (join->condition->opType != hsql::kOpEquals) {
            printf("Unsupported Syntax!\n");
            return nullptr;
        }
        if (join->condition->expr->type != hsql::kExprColumnRef) {
            printf("Unsupported Syntax!\n");
            return nullptr;
        }
        if (join->condition->expr2->type != hsql::kExprColumnRef) {
            printf("Unsupported Syntax!\n");
            return nullptr;
        }

        // Join two tables with all columns (no duplication)
        Schema *schema = new Schema();
        schema->n_columns = child1->schema->n_columns;
        for (int i = 0; i < child1->schema->n_columns; i++) {
            schema->column_name.push_back(child1->schema->column_name[i]);
        }
        for (int i = 0; i < child2->schema->n_columns; i++) {
            bool found = false;
            std::string column = child2->schema->column_name[i];
            for (int j = 0; j < schema->n_columns; j++) {
                if (column == schema->column_name[j]) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                schema->n_columns++;
                schema->column_name.push_back(column);
            } else {
                // TBD: for duplicated columns, join key should be dropped and others should be renamed
                // but here we dropped all of them for simplicity
            }
        }

        std::shared_ptr<Treenode> ret = std::make_shared<Treenode>(Treenode::Operation::JOIN, child1, child2);
        ret->schema = schema;
        ret->joinDescription = std::make_shared<KeyDescription>();
        ret->joinDescription->key1 = join->condition->expr->name;
        ret->joinDescription->key2 = join->condition->expr2->name;
        return ret;
    }

    // (recursively) build condition expression
    std::shared_ptr<Expr> build_condition(hsql::Expr *expr) {
        if (expr->type == hsql::kExprOperator && expr->opType == hsql::kOpAnd) {
            std::shared_ptr<Expr> ret = std::make_shared<Expr>(Expr::AND);
            ret->child1 = build_condition(expr->expr);
            ret->child2 = build_condition(expr->expr2);
            return ret;
        }
        if (expr->type == hsql::kExprOperator && expr->opType == hsql::kOpOr) {
            std::shared_ptr<Expr> ret = std::make_shared<Expr>(Expr::OR);
            ret->child1 = build_condition(expr->expr);
            ret->child2 = build_condition(expr->expr2);
            return ret;
        }
        if (expr->type == hsql::kExprOperator && expr->opType == hsql::kOpEquals) {
            std::shared_ptr<Expr> ret = std::make_shared<Expr>(Expr::EQUALS);
            ret->child1 = build_condition(expr->expr);
            ret->child2 = build_condition(expr->expr2);
            return ret;
        }
        if (expr->type == hsql::kExprOperator && expr->opType == hsql::kOpNotEquals) {
            std::shared_ptr<Expr> ret = std::make_shared<Expr>(Expr::NOTEQUALS);
            ret->child1 = build_condition(expr->expr);
            ret->child2 = build_condition(expr->expr2);
            return ret;
        }

        // Parser treats both "123" and 123 as Int, but actually they are String. Same for Float
        if (expr->type == hsql::kExprLiteralInt) {
            std::shared_ptr<Expr> ret = std::make_shared<Expr>(Expr::CONSTVALUE);
            ret->const_string = std::to_string(expr->ival);
            return ret;
        }
        if (expr->type == hsql::kExprLiteralFloat) {
            std::shared_ptr<Expr> ret = std::make_shared<Expr>(Expr::CONSTVALUE);
            ret->const_string = std::to_string(expr->fval);
            return ret;
        }
        // For String, we have to deduce whether it is a column_name or a const_string
        if (expr->type == hsql::kExprColumnRef) {
            std::shared_ptr<Expr> ret = std::make_shared<Expr>();
            std::string name = expr->name;
            bool found = false;

            // Check the schemas for the column name
            for (int i = 0; i < user_schema->n_columns; i++) {
                if (name == user_schema->column_name[i]) {
                    found = true;
                    break;
                }
            }
            for (int i = 0; i < article_schema->n_columns; i++) {
                if (name == article_schema->column_name[i]) {
                    found = true;
                    break;
                }
            }
            for (int i = 0; i < user_read_schema->n_columns; i++) {
                if (name == user_read_schema->column_name[i]) {
                    found = true;
                    break;
                }
            }
            for (int i = 0; i < be_read_schema->n_columns; i++) {
                if (name == be_read_schema->column_name[i]) {
                    found = true;
                    break;
                }
            }
            for (int i = 0; i < popular_rank_schema->n_columns; i++) {
                if (name == popular_rank_schema->column_name[i]) {
                    found = true;
                    break;
                }
            }

            // Assign column or constant value
            if (found) {
                ret->type = Expr::TABLEVALUE;
                ret->column_name = name;
            } else {
                ret->type = Expr::CONSTVALUE;
                ret->const_string = name;
            }

            return ret;
        }

        printf("Unsupported Syntax!\n");
        return nullptr;
    }

    // Do some filters or projects on a single table on control node
    std::shared_ptr<Treenode> build_plan_tree_select(const hsql::SelectStatement *stmt) {
        std::shared_ptr<Treenode> source;

        // source
        if (stmt->fromTable->type == hsql::kTableSelect) {
            source = build_plan_tree_select(stmt->fromTable->select);
        } else if (stmt->fromTable->type == hsql::kTableJoin) {
            source = build_plan_tree_join(stmt->fromTable->join);
        } else if (stmt->fromTable->type == hsql::kTableName) {
            source = build_plan_tree_table(stmt->fromTable);
        } else {
            printf("Unsupported Syntax!\n");
            return nullptr;
        }

        // search conditions
        std::shared_ptr<Treenode> filter_result;
        if (stmt->whereClause == nullptr) {
            filter_result = source;  // no search condition
        } else {
            std::shared_ptr<Expr> expr = build_condition(stmt->whereClause);
            filter_result = std::make_shared<Treenode>(Treenode::Operation::FILTER, source, nullptr);
            filter_result->schema = source->schema;
            filter_result->filterDescription = std::make_shared<FilterDescription>(expr);
        }

        // fields
        std::shared_ptr<Treenode> project_result;
        if ((*stmt->selectList).size() == 1 && (*stmt->selectList)[0]->type == hsql::kExprStar) {
            project_result = filter_result;  // no need to do project for "select * from xxx"
        } else {
            Schema *schema = new Schema();
            schema->n_columns = 0;
            for (hsql::Expr *expr : *stmt->selectList) {
                if (expr->type == hsql::kExprStar) {
                    schema->n_columns += filter_result->schema->n_columns;
                    for (int i = 0; i < filter_result->schema->n_columns; i++) {
                        schema->column_name.push_back(filter_result->schema->column_name[i]);
                    }
                } else if (expr->type == hsql::kExprColumnRef) {
                    schema->n_columns++;
                    schema->column_name.push_back(expr->name);
                }
            }
            project_result = std::make_shared<Treenode>(Treenode::Operation::PROJECT, filter_result, nullptr);
            project_result->schema = schema;
        }

        // limit
        std::shared_ptr<Treenode> limit_result;
        if (stmt->limit == nullptr) {
            limit_result = project_result;  // no limit
        } else {
            if (stmt->limit->limit->type != hsql::kExprLiteralInt) {
                printf("Unsupported Syntax!\n");
                return nullptr;
            }
            int limit_num = stmt->limit->limit->ival;
            limit_result = std::make_shared<Treenode>(Treenode::Operation::LIMIT, project_result, nullptr);
            limit_result->schema = project_result->schema;
            limit_result->limitDescription = std::make_shared<LimitDescription>(limit_num);
        }

        return limit_result;
    }

    Table *process_query(std::string query) {
        // get AST
        hsql::SQLParserResult result;
        hsql::SQLParser::parse(query, &result);

        // input sql must be a select statement
        if (result.getStatement(0)->type() != hsql::kStmtSelect) {
            printf("Unsupported Syntax!\n");
            return nullptr;
        }
        const hsql::SelectStatement *stmt = (const hsql::SelectStatement *)result.getStatement(0);
        // hsql::printStatementInfo(stmt);

        std::shared_ptr<Treenode> root = build_plan_tree_select(stmt);
        Table *t = execute_node(root);
        return t;
    }
};

#endif  // RPC_SERVER_H