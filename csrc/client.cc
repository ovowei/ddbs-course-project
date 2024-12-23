#include <iostream>
#include <string>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <sstream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


static constexpr size_t kAppMaxConcurrency = 1;
static constexpr uint8_t kqueryType = 2;
static constexpr uint8_t kbereadType = 3;
static constexpr uint8_t kpopularType = 4;
static constexpr uint8_t kdump0Type = 5;
static constexpr uint8_t kdump1Type = 6;
static constexpr uint8_t kmonitorType = 7;
static constexpr uint8_t kregisterStandbyType = 8;
static constexpr uint8_t kexitType = 9;
static constexpr uint8_t kinvalidType = 10;

static constexpr size_t kMsgSize = 1024;
std::string clientname = "127.0.0.1";
std::string servername = "127.0.0.1";
uint16_t serverport = 31813;
uint16_t clientport = 31814;



// 函数：解析命令和参数
uint8_t parseCommand(const std::string &input, std::string &params) {
    std::istringstream iss(input);
    std::string cmd;
    std::getline(iss, cmd, ' ');  // 获取命令部分
    params = input.substr(cmd.length() + 1);  // 获取命令后面的参数部分

    if (cmd == "query") return kqueryType;
    if (cmd == "beread") return kbereadType;
    if (cmd == "popular") return kpopularType;
    if (cmd == "dump0") return kdump0Type;
    if (cmd == "dump1") return kdump1Type;
    if (cmd == "monitor") return kmonitorType;
    if (cmd == "register") return kregisterStandbyType;
    if (cmd == "exit") return kexitType;
    return kinvalidType;
}


// 函数：根据命令类型生成对应的消息内容
std::string getMessageForCommand(uint8_t cmd, const std::string &params) {
    switch (cmd) {
        case kqueryType:
            return "QUERY " + params;
        case kbereadType:
            return "BEREAD " + params;
        case kpopularType:
            return "POPULAR " + params;
        case kdump0Type:
            return "DUMP0";
        case kdump1Type:
            return "DUMP1";
        case kmonitorType:
            return "MONITOR";
        case kregisterStandbyType:
            return "REGISTER";
        case kexitType:
            return "EXIT";
        default:
            return "INVALID";
    }
}



// 函数：发送UDP消息
void sendMessage(int sockfd, const std::string &message, const struct sockaddr_in &serverAddr) {
    std::cout << message << std::endl;
    if (sendto(sockfd, message.c_str(), message.length(), 0, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) == -1) {
        perror("sendto failed");
    }
}
// 函数：解析返回的消息
void processMessage(const std::string &message) {
    std::cout << "Processing received message: " << message << std::endl;
}

// 主函数
int main() {
    int sockfd;
    struct sockaddr_in serverAddr, clientAddr;
    char buffer[kMsgSize];
    socklen_t serverAddrLen = sizeof(serverAddr);

    // 创建UDP socket
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
        perror("socket failed");
        return 1;
    }

    // 配置客户端地址
    memset(&clientAddr, 0, sizeof(clientAddr));
    clientAddr.sin_family = AF_INET;
    clientAddr.sin_port = htons(clientport);
    if (inet_pton(AF_INET, servername.c_str(), &clientAddr.sin_addr) <= 0) {
        perror("Invalid server address");
        return 1;
    }

    // 配置服务器地址
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(serverport);
    if (inet_pton(AF_INET, servername.c_str(), &serverAddr.sin_addr) <= 0) {
        perror("Invalid server address");
        return 1;
    }

    std::string input;
    
    while (true) {
        std::cout << "Enter a command:" << std::endl;
        std::cout << "    QUERY <query statement>" << std::endl;
        std::cout << "    BEREAD" << std::endl;
        std::cout << "    POPULAR" << std::endl;
        std::cout << "    MONITOR" << std::endl;
        std::cout << "    REGISTER <HOST:port>, <user>, <password>, <schema>, such as \"REGISTER 127.0.0.1:3312, root, 123456, standby1,\" notice the last comma" << std::endl;
        std::cout << "    DUMP <node_num>" << std::endl;
        std::cout << "    EXIT" << std::endl;
        std::cout << "Command: ";
        std::getline(std::cin, input);
        
        if (input == "EXIT") {
            break;
        }

        // 发送命令到后端服务器
        if (sendto(sockfd, input.c_str(), input.length(), 0, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) == -1) {
            perror("sendto failed");
            continue;
        }

        // 接收后端响应
        ssize_t recvLen = recvfrom(sockfd, buffer, kMsgSize, 0, (struct sockaddr *)&clientAddr, &serverAddrLen);
        if (recvLen == -1) {
            perror("recvfrom failed");
            continue;
        }

        std::string receivedMessage(buffer, recvLen);
        std::cout << "Result: \n" << receivedMessage << std::endl;

        // 处理收到的消息
        // processMessage(receivedMessage);

        
    }

    close(sockfd);
    return 0;
}