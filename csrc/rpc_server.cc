#include "rpc_server.h"

#include <sys/time.h>

#include "rpc_client.h"

// Control 对象的初始化需要参数
Control control("127.0.0.1:3310", "root", "123456", "primary1",   // 第一个 Primary DB 参数
                "127.0.0.1:3311", "root", "123456", "primary2");  // 第二个 Primary DB 参数

inline double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + (tv.tv_usec / 1e6);
}

double t;
erpc::Rpc<erpc::CTransport> *rpc;
std::string a;
std::vector<std::string> aid;
std::vector<std::string> text;
std::vector<std::string> image;
std::vector<std::string> video;
static uint8_t flag = 0;

// 其余的请求处理函数不变
void req_handler_read(erpc::ReqHandle *req_handle, void *) {
    auto &resp = req_handle->pre_resp_msgbuf_;
    char *buf = (char *)resp.buf_;
    auto req_query = req_handle->get_req_msgbuf();
    std::string squery((char *)req_query->buf_);
    Table *res = control.process_query(squery);
    size_t offset = 0;
    res->set_buf(buf, offset);
    flag = 1;
    rpc->resize_msg_buffer(&resp, offset);
    rpc->enqueue_response(req_handle, &resp);
}

void set_resp(const std::string &s, char *buf, size_t &offset) {
    memcpy(buf + offset, s.c_str(), s.length());
    offset += s.length();
}

void req_handler_beread(erpc::ReqHandle *req_handle, void *) {
    auto &resp = req_handle->pre_resp_msgbuf_;
    control.populate_be_read();
    rpc->resize_msg_buffer(&resp, 8);
    rpc->enqueue_response(req_handle, &resp);
}

void req_handler_popular(erpc::ReqHandle *req_handle, void *) {
    auto &resp = req_handle->pre_resp_msgbuf_;
    control.populate_popular_rank(1506000005000ll, "monthly", aid, text, image, video);
    size_t offset = 0;
    char *buf = (char *)resp.buf_;
    for (int i = 0; i < 5; i++) {
        set_resp(aid[i], buf, offset);
        std::string del = ",";
        set_resp(del, buf, offset);
    }

    rpc->resize_msg_buffer(&resp, offset);
    rpc->enqueue_response(req_handle, &resp);
}

void req_handler_register(erpc::ReqHandle *req_handle, void *) {
    auto &resp = req_handle->pre_resp_msgbuf_;
    char *buf = (char *)req_handle->get_req_msgbuf()->buf_;

    // 解析传入的 Standby 节点信息
    std::string data(buf);
    std::istringstream ss(data);
    std::string host, user, password, schema;
    std::getline(ss, host, ',');
    std::getline(ss, user, ',');
    std::getline(ss, password, ',');
    std::getline(ss, schema, ',');

    // 调用 Control 层的 add_standby_db
    try {
        control.add_standby_db(host, user, password, schema);
        strcpy(buf, "Standby registered successfully!");
    } catch (const std::exception &e) {
        strcpy(buf, "Failed to register standby: ");
        strcat(buf, e.what());
    }

    rpc->resize_msg_buffer(&resp, strlen(buf) + 1);
    rpc->enqueue_response(req_handle, &resp);
}

void req_handler_monitor(erpc::ReqHandle *req_handle, void *) {
    printf("go into monitor handler\n");
    auto &resp = req_handle->pre_resp_msgbuf_;
    char *buf = (char *)resp.buf_;

    try {
        size_t offset = 0;
        control.get_monitoring_info(buf, offset);
        rpc->resize_msg_buffer(&resp, offset);
    } catch (const std::exception &e) {
        strcpy(buf, "Failed to retrieve monitoring info: ");
        strcat(buf, e.what());
        rpc->resize_msg_buffer(&resp, strlen(buf) + 1);
    }
    rpc->enqueue_response(req_handle, &resp);
}

void req_handler_dump(erpc::ReqHandle *req_handle, void *context) {
    auto &resp = req_handle->pre_resp_msgbuf_;
    char *buf = (char *)req_handle->get_req_msgbuf()->buf_;
    int primary_index = std::stoi(buf);

    try {
        control.manual_dump_primary(primary_index);
        sprintf(buf, "Primary DB %d dumped successfully!", primary_index);
    } catch (const std::exception &e) {
        sprintf(buf, "Failed to dump Primary DB %d: %s", primary_index, e.what());
    }
    rpc->resize_msg_buffer(&resp, strlen(buf) + 1);
    rpc->enqueue_response(req_handle, &resp);
}

int main(int argc, char *argv[]) {
    control.bulk_load(1);
    control.bulk_load(2);

    std::string host = "127.0.0.1:3312";
    std::string user = "root";
    std::string password = "123456";
    std::string schema = "standby1";
    control.add_standby_db(host, user, password, schema);
    control.get_monitoring_info();

    control.populate_be_read();

    std::vector<std::string> aid;
    std::vector<std::string> text;
    std::vector<std::string> image;
    std::vector<std::string> video;
    control.populate_popular_rank(1506000005000ll, "monthly", aid, text, image, video);
    for (int i = 0; i < 5; i++) {
        printf("aid = %s, text = %s, image = %s, video = %s\n", aid[i].c_str(), text[i].c_str(), image[i].c_str(), video[i].c_str());
    }

    control.get_monitoring_info();

    // std::string query = "select * from user limit 10";
    std::string query =
        "select uid, gender, aid, title, articleTags, agreeOrNot from article join (select uid, aid, agreeOrNot, commentOrNot, shareOrNot from user_read) on "
        "aid=aid join user on uid=uid where agreeOrNot = 1 and gender = male limit 10";
    std::cout << "process query: " << query << std::endl;
    Table *res = control.process_query(query);
    res->print();

    // std::string server_uri = servername + ":" + std::to_string(serverport);
    // // std::cout<<servername<<" "<<serverport<<std::endl;
    // erpc::Nexus nexus(server_uri);

    // nexus.register_req_func(kreadType, req_handler_read);
    // nexus.register_req_func(kbereadType, req_handler_beread);
    // nexus.register_req_func(kpopularType, req_handler_popular);
    // nexus.register_req_func(kregisterStandbyType, req_handler_register);
    // nexus.register_req_func(kmonitorType, req_handler_monitor);
    // nexus.register_req_func(kdump0Type, req_handler_dump);
    // nexus.register_req_func(kdump1Type, req_handler_dump);

    // rpc = new erpc::Rpc<erpc::CTransport>(&nexus, nullptr, 0, nullptr);

    // while (true) {
    //     rpc->run_event_loop_once();
    // }
}
