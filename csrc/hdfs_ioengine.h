#include <cstdlib>
#include <iostream>
#include <string>

class FS_ENGINE {
   public:
    // Upload binlog file to HDFS
    int upload_binlog_to_hdfs(const std::string &local_binlog_path, const std::string &hdfs_binlog_path) {
        std::string command = "hdfs dfs -put " + local_binlog_path + " " + hdfs_binlog_path;
        int ret = system(command.c_str());
        if (ret != 0) {
            std::cerr << "Error uploading binlog to HDFS!" << std::endl;
            return -1;
        }
        std::cout << "Successfully uploaded binlog to HDFS: " << hdfs_binlog_path << std::endl;
        return 0;
    }

    // Download binlog from HDFS to local machine
    int download_binlog_from_hdfs(const std::string &hdfs_binlog_path, const std::string &local_binlog_path) {
        std::string command = "hdfs dfs -get " + hdfs_binlog_path + " " + local_binlog_path;
        int ret = system(command.c_str());
        if (ret != 0) {
            std::cerr << "Error downloading binlog from HDFS!" << std::endl;
            return -1;
        }
        std::cout << "Successfully downloaded binlog from HDFS: " << hdfs_binlog_path << std::endl;
        return 0;
    }

    // Read binlog file from HDFS
    int read_binlog_from_hdfs(const std::string &hdfs_binlog_path) {
        std::string command = "hdfs dfs -cat " + hdfs_binlog_path;
        int ret = system(command.c_str());
        if (ret != 0) {
            std::cerr << "Error reading binlog from HDFS!" << std::endl;
            return -1;
        }
        std::cout << "Successfully read binlog from HDFS: " << hdfs_binlog_path << std::endl;
        return 0;
    }

    // Download article details from HDFS
    int download_article_details_from_hdfs(const std::string &aid) {
        // Ensure no extra spaces or redundant + operators
        std::string command = "hdfs dfs -get /data/articles/" + aid + "/* /tmp/article_details/";
        int ret = system(command.c_str());
        if (ret != 0) {
            std::cerr << "Error downloading article details from HDFS!" << std::endl;
            return -1;
        }
        std::cout << "Successfully downloaded article details from HDFS: " << aid << std::endl;
        return 0;
    }

    // Upload article details to HDFS
    int upload_article_details_to_hdfs(const std::string &local_article_details_path) {
        // Check if the directory exists, if not create it
        std::string command = "hdfs dfs -mkdir -p data";  // Ensure the target directory exists
        int ret = system(command.c_str());
        if (ret != 0) {
            std::cerr << "Error creating directory in HDFS!" << std::endl;
            return -1;
        }

        // Upload the article details file
        command = "hdfs dfs -put " + local_article_details_path + " data/";
        ret = system(command.c_str());
        if (ret != 0) {
            std::cerr << "Error uploading article details to HDFS!" << std::endl;
            return -1;
        }

        std::cout << "Successfully uploaded article details to HDFS: " << local_article_details_path << std::endl;
        return 0;
    }
};
