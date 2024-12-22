#include "rpc_client.h"

#include <fstream>
#include <iostream>
#include <string>

#include "hdfs_ioengine.h"
#include "rpc_server.h"

static uint8_t flag = 0;  // 请求标志，用于跟踪请求类型
char dummy[] = "test";    // 示例数据
Timer timer;
FS_ENGINE fs;

// Continuation function，输出请求类型、返回结果和执行时间
void cont_func(void *_context, void *_tag) {
    auto resp = rpcclient.resp_msgbuf[0];
    char *buf = (char *)(resp.buf_);
    double exec_time = timer.end();  // 获取执行时间

    std::string request_type;
    switch (flag) {
        case kreadType:
            request_type = "Read Query";
            break;
        case kbereadType:
            request_type = "Populate Be-Read";
            break;
        case kpopularType:
            request_type = "Populate Popular Rank";
            break;
        case kdump0Type:
            request_type = "Dump Primary 0";
            break;
        case kdump1Type:
            request_type = "Dump Primary 1";
            break;
        case kmonitorType:
            request_type = "Monitor Info";
            break;
        case kregisterStandbyType:
            request_type = "Register Standby";
            break;
        default:
            request_type = "Unknown";
    }

    // 输出结果
    std::cout << "Request Type: " << request_type << std::endl;
    std::cout << "Execution Time: " << exec_time << " seconds" << std::endl;
    std::cout << "Response:\n" << buf << std::endl;
    return;
}

int main(int argc, char *argv[]) {
    // 从终端获取参数
    std::string clientname = "10.0.2.185";
    std::string servername = "10.0.2.183";
    uint16_t kUDPPort = 31850;

    if (argc > 1) clientname = argv[1];
    if (argc > 2) servername = argv[2];
    if (argc > 3) kUDPPort = std::stoi(argv[3]);

    Rpcclient rpcclient(0, clientname, kUDPPort);
    rpcclient.Activate_client(servername);

    std::string input;
    std::string query = "";  // 默认查询为空字符串

    while (true) {
        // 提示用户输入请求类型
        std::cout << "Enter request type (read, beread, popular, dump0, dump1, monitor, register): ";
        std::cin >> input;

        if (input == "read") {
            flag = kreadType;
            std::cout << "Enter your query (press enter for default query): ";
            std::cin.ignore();  // 清空输入缓冲区
            std::getline(std::cin, query);

            // 如果用户未输入查询，使用默认查询字符串
            if (query.empty()) {
                query =
                    "select uid, gender, aid, title, articleTags, agreeOrNot from article join (select uid, aid, agreeOrNot, commentOrNot, shareOrNot from "
                    "user_read) on aid=aid join user on uid=uid where agreeOrNot = 1 and gender = male limit 10";
            }

            rpcclient.send_req(query.c_str(), query.length(), 0, kreadType);
        } else if (input == "beread") {
            flag = kbereadType;
            rpcclient.send_req(dummy, sizeof(dummy), 0, kbereadType);
        } else if (input == "popular") {
            flag = kpopularType;
            rpcclient.send_req(dummy, sizeof(dummy), 0, kpopularType);
        } else if (input == "dump0") {
            flag = kdump0Type;

            // 发送 Primary 0 的索引
            char primary_index[] = "0";  // Primary 0
            rpcclient.send_req(primary_index, sizeof(primary_index), 0, kdump0Type);
        } else if (input == "dump1") {
            flag = kdump1Type;

            // 发送 Primary 1 的索引
            char primary_index[] = "1";  // Primary 1
            rpcclient.send_req(primary_index, sizeof(primary_index), 0, kdump1Type);
        } else if (input == "monitor") {
            flag = kmonitorType;
            rpcclient.send_req(dummy, sizeof(dummy), 0, kmonitorType);
        } else if (input == "register") {
            flag = kregisterStandbyType;

            // 用户输入 Standby 节点信息
            std::string host, user, password, schema;
            std::cout << "Enter Standby Host: ";
            std::cin >> host;
            std::cout << "Enter Standby User: ";
            std::cin >> user;
            std::cout << "Enter Standby Password: ";
            std::cin >> password;
            std::cout << "Enter Standby Schema: ";
            std::cin >> schema;

            // 将信息拼接成单个字符串并发送
            std::string register_data = host + "," + user + "," + password + "," + schema;
            rpcclient.send_req(register_data.c_str(), register_data.length(), 0, kregisterStandbyType);
        } else if (input == "exit") {
            std::cout << "Exiting..." << std::endl;
            break;
        } else {
            std::cout << "Invalid input. Please try again." << std::endl;
            continue;
        }

        // 开始计时并轮询响应
        timer.begin();
        while (flag) rpcclient.poll();  // 等待响应
    }

    return 0;
}
