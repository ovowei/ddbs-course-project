#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "rpc_server.h"


Control control("sapphire2:6613", "root", "ddbsproject", "primary1",   // 第一个 Primary DB 参数
                "sapphire3:6613", "root", "ddbsproject", "primary2");  // 第二个 Primary DB 参数


static constexpr size_t kMsgSize = 2048;
static constexpr uint16_t kServerPort = 31813; // 服务器端口

// 定义支持的命令
enum Command {
    QUERY,
    BEREAD,
    POPULAR,
    DUMP0,
    DUMP1,
    MONITOR,
    REGISTER,
    EXIT,
    INVALID
};

// 函数：解析命令
Command parseCommand(const std::string &cmd) {
    if (cmd == "QUERY") return QUERY;
    if (cmd == "BEREAD") return BEREAD;
    if (cmd == "POPULAR") return POPULAR;
    if (cmd == "DUMP0") return DUMP0;
    if (cmd == "DUMP1") return DUMP1;
    if (cmd == "MONITOR") return MONITOR;
    if (cmd == "REGISTER") return REGISTER;
    if (cmd == "EXIT") return EXIT;
    return INVALID;
}

void set_resp(const std::string &s, char *buf, size_t &offset) {
    memcpy(buf + offset, s.c_str(), s.length());
    offset += s.length();
}

std::string handleQuery(const std::string &params) {
    Table *res;
    char buf[kMsgSize];
    size_t offset = 0;

    std::cout << "query request:" + params << std::endl;
    res = control.process_query(params);
    if(res != nullptr) {
        res->set_buf(buf, offset);
        return std::string(buf, offset); 
    } 
    return "Unsupported Syntax!";
}

std::string handlePopular(const std::string &params) {
    std::cout << "popular request:" + params << std::endl;
    std::vector<std::string> aid;
    std::vector<std::string> text;
    std::vector<std::string> image;
    std::vector<std::string> video;

    control.populate_popular_rank(1506000005000ll, "monthly", aid, text, image, video);
    char buf[kMsgSize];
    size_t offset = 0;

    for (int i = 0; i < 5; i++) {
        set_resp(aid[i], buf, offset);
        std::string del = ",";
        set_resp(del, buf, offset);
    }

    return std::string(buf, offset); 

}

std::string handleMonitor(const std::string &params) {
    std::cout << "monitor request:" + params << std::endl;
    char buf[kMsgSize];
    size_t offset = 0;
    control.get_monitoring_info(buf,offset);
    return std::string(buf, offset); 
}

std::string handleRegister(const std::string &params) {
    char buf[kMsgSize];
    size_t offset = 0;

    std::cout << "Register request:" + params << std::endl;
    // 解析传入的 Standby 节点信息
    std::string data(params);
    std::istringstream ss(data);
    std::string host, user, password, schema;
    std::getline(ss, host, ',');
    std::getline(ss, user, ',');
    std::getline(ss, password, ',');
    std::getline(ss, schema, ',');

    try {
        control.add_standby_db(host, user, password, schema);
        strcpy(buf, "Standby registered successfully!");
    } catch (const std::exception &e) {
        strcpy(buf, "Failed to register standby: ");
        strcat(buf, e.what());
    }

    return std::string(buf, offset); 
}

std::string handleDump(const std::string &params) {
    return "Dump core finished!";
}


// 函数：处理命令并生成响应
std::string handleRequest(Command cmd, const std::string &params) {
    

    


    switch (cmd) {
        case QUERY:
            return handleQuery(params);
        case BEREAD:
            std::cout << "beread request:" << std::endl;
            control.populate_be_read();
            return "Table `beread` has been ppopulated";
        case POPULAR:
            return handlePopular(params);
        case DUMP0:
            return handleDump(params);  
        case DUMP1:
            return handleDump(params);  
        case MONITOR:
            return handleMonitor(params);  
        case REGISTER:
            return handleRegister(params);  
        default:
            return "Invalid Command";
    }
}


// UDP Server 
int udpServer(){
    int sockfd;
    struct sockaddr_in serverAddr, clientAddr;
    socklen_t clientAddrLen = sizeof(clientAddr);
    char buffer[kMsgSize];

    // 创建UDP socket
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
        perror("socket failed");
        return 1;
    }

    // 配置服务器地址
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(kServerPort);
    serverAddr.sin_addr.s_addr = INADDR_ANY;  // 接受任何客户端的连接

    // 绑定服务器地址到套接字
    if (bind(sockfd, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) == -1) {
        perror("bind failed");
        close(sockfd);
        return 1;
    }

    std::cout << "Server is running and waiting for requests on port " << kServerPort << "...\n";

    while (true) {
        // 清空缓冲区
        memset(buffer, 0, kMsgSize);

        // 接收来自客户端的消息
        ssize_t recvLen = recvfrom(sockfd, buffer, kMsgSize, 0, (struct sockaddr *)&clientAddr, &clientAddrLen);
        if (recvLen == -1) {
            perror("recvfrom failed");
            continue;
        }

        // 获取客户端的IP和端口
        char clientIP[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(clientAddr.sin_addr), clientIP, INET_ADDRSTRLEN);
        uint16_t clientPort = ntohs(clientAddr.sin_port);
        
        std::string receivedMessage(buffer, recvLen);
        std::cout << "Received message from " << clientIP << ":" << clientPort << " -> " << receivedMessage << std::endl;

        // 解析消息并处理
        std::istringstream iss(receivedMessage);
        std::string cmd;
        std::getline(iss, cmd, ' ');  // 提取命令部分
        std::string params = "";
        if (receivedMessage.length() > cmd.length()) {
            // 只有当命令后还有字符时，才截取参数部分
            params = receivedMessage.substr(cmd.length() + 1); // 提取命令后面的参数
        }

        Command command = parseCommand(cmd);
        std::cout << params << std::endl;
        std::string response = handleRequest(command, params);

        // 发送响应到客户端
        ssize_t sentLen = sendto(sockfd, response.c_str(), response.length(), 0, (struct sockaddr *)&clientAddr, clientAddrLen);
        if (sentLen == -1) {
            perror("sendto failed");
        }
    }

    close(sockfd);
    return 0;
}


// 主函数
int main() {
    control.bulk_load(1);
    control.bulk_load(2);

    udpServer();

    return 0;
}