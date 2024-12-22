#include <string>
#include <iostream>
#include <cstdlib>

class FS_ENGINE {
public:
    // Upload binlog file to HDFS
    int upload_binlog_to_hdfs(std::string local_binlog_path, std::string hdfs_binlog_path) {
        std::string command = "../hadoop3.3.3/bin/hadoop fs -put " + local_binlog_path + " " + hdfs_binlog_path;
        int ret = system(command.c_str());
        if (ret != 0) {
            std::cerr << "Error uploading binlog to HDFS!" << std::endl;
            return -1;
        }
        std::cout << "Successfully uploaded binlog to HDFS: " << hdfs_binlog_path << std::endl;
        return 0;
    }

    // Download binlog from HDFS to local machine
    int download_binlog_from_hdfs(std::string hdfs_binlog_path, std::string local_binlog_path) {
        std::string command = "../hadoop3.3.3/bin/hadoop fs -get " + hdfs_binlog_path + " " + local_binlog_path;
        int ret = system(command.c_str());
        if (ret != 0) {
            std::cerr << "Error downloading binlog from HDFS!" << std::endl;
            return -1;
        }
        std::cout << "Successfully downloaded binlog from HDFS: " << hdfs_binlog_path << std::endl;
        return 0;
    }

    // Read binlog file from HDFS
    int read_binlog_from_hdfs(std::string hdfs_binlog_path) {
        std::string command = "../hadoop3.3.3/bin/hadoop fs -cat " + hdfs_binlog_path;
        int ret = system(command.c_str());
        if (ret != 0) {
            std::cerr << "Error reading binlog from HDFS!" << std::endl;
            return -1;
        }
        std::cout << "Successfully read binlog from HDFS: " << hdfs_binlog_path << std::endl;
        return 0;
    }
};
