#include "rpc_server.h"

#include <sys/time.h>

#include "rpc_client.h"

inline double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + (tv.tv_usec / 1e6);
}

double t;
erpc::Rpc<erpc::CTransport> *rpc;
Control control;
std::string a;
std::vector<std::string> aid;
std::vector<std::string> text;
std::vector<std::string> image;
std::vector<std::string> video;
static uint8_t flag = 0;
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
    // puts("handle read");
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
    // puts("handle beread");
}

void req_handler_popular(erpc::ReqHandle *req_handle, void *) {
    // puts("in popular");
    auto &resp = req_handle->pre_resp_msgbuf_;
    control.populate_popular_rank(1506000005000ll, "monthly", aid, text, image, video);
    size_t offset = 0;
    char *buf = (char *)resp.buf_;
    for (int i = 0; i < 5; i++) {
        set_resp(aid[i], buf, offset);
        std::string del = ",";
        set_resp(del, buf, offset);
        // set_resp(text[i], buf, offset);
        // set_resp(image[i], buf, offset);
        // set_resp(video[i], buf, offset);
    }

    rpc->resize_msg_buffer(&resp, offset);
    rpc->enqueue_response(req_handle, &resp);
    // puts("handle popular\n");
}

int main(int argc, char *argv[]) {
    // Control control;
    // task1
    control.bulk_load(1);
    control.bulk_load(2);
    std::string server_uri = servername + ":" + std::to_string(kUDPPort);
    erpc::Nexus nexus(server_uri);
    nexus.register_req_func(kreadType, req_handler_read);
    nexus.register_req_func(kbereadType, req_handler_beread);
    nexus.register_req_func(kpopularType, req_handler_popular);
    rpc = new erpc::Rpc<erpc::CTransport>(&nexus, nullptr, 0, nullptr);
    while (true) {
        rpc->run_event_loop_once();
    }
    // task 2
    // std::string query = "select uid, gender, aid, title, articleTags, agreeOrNot from article join (select uid, aid, agreeOrNot, commentOrNot, shareOrNot
    // from user_read) on aid=aid join user on uid=uid where agreeOrNot = 1 and gender = male limit 10"; std::cout << "process query: " << query << std::endl;
    // Table * res = control.process_query(query);
    // res->print();

    // // task 3
    // control.populate_be_read();

    // // task 4
    // std::vector<std::string> aid;
    // std::vector<std::string> text;
    // std::vector<std::string> image;
    // std::vector<std::string> video;
    // control.populate_popular_rank(1506000005000ll, "monthly", aid, text, image, video);
    // for (int i=0;i<5;i++) {
    //     printf("aid = %s, text = %s, image = %s, video = %s\n", aid[i].c_str(), text[i].c_str(), image[i].c_str(), video[i].c_str());
    // }
}