#ifndef RPCCLIENT_H
#define RPCCLIENT_H
#include <bits/stdc++.h>

#include "rpc.h"

// 增加四种新类型
static constexpr size_t kAppMaxConcurrency = 1;
static constexpr uint8_t kreadType = 2;
static constexpr uint8_t kbereadType = 3;
static constexpr uint8_t kpopularType = 4;
static constexpr uint8_t kdump0Type = 5;
static constexpr uint8_t kdump1Type = 6;
static constexpr uint8_t kmonitorType = 7;
static constexpr uint8_t kregisterStandbyType = 8;

static constexpr size_t kMsgSize = 1024;

std::string clientname = "10.0.2.185";
std::string servername = "10.0.2.183";
uint16_t kUDPPort = 31850;

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}
void cont_func(void *_context, void *_tag);

class Rpcclient {
   public:
    int thread_id = -1;
    size_t stat_tx_reqs_tot = 0;
    size_t stat_rx_reqs_tot = 0;
    erpc::MsgBuffer req_msgbuf[kAppMaxConcurrency];
    erpc::MsgBuffer resp_msgbuf[kAppMaxConcurrency];

    Rpcclient(int tid, const std::string &cli, uint16_t port) : thread_id(tid), client_name(cli), kUDPPort(port) {
        client_uri = client_name + ":" + std::to_string(kUDPPort);
        nexus = new erpc::Nexus(client_uri);
        rpc = new erpc::Rpc<erpc::CTransport>(nexus, nullptr, 0, sm_handler);
    }

    ~Rpcclient() {}

    void Activate_client(const std::string &ser) {
        server_name = ser;
        server_uri = ser + ":" + std::to_string(kUDPPort);
        session_num = rpc->create_session(server_uri, 0);
        while (!rpc->is_connected(session_num)) rpc->run_event_loop_once();
        for (int i = 0; i < kAppMaxConcurrency; ++i) {
            req_msgbuf[i] = rpc->alloc_msg_buffer_or_die(kMsgSize);
            resp_msgbuf[i] = rpc->alloc_msg_buffer_or_die(kMsgSize);
        }
    }

    void send_req(const char *usrdata, size_t len, int slot, uint8_t reqtype) {
        memcpy(req_msgbuf[slot].buf_, usrdata, len);
        rpc->resize_msg_buffer(&req_msgbuf[slot], len);
        rpc->enqueue_request(session_num, reqtype, &req_msgbuf[slot], &resp_msgbuf[slot], cont_func, reinterpret_cast<void *>(slot));
    }

    void poll() { rpc->run_event_loop_once(); }

   private:
    uint16_t kUDPPort;
    erpc::Rpc<erpc::CTransport> *rpc = nullptr;
    erpc::Nexus *nexus = nullptr;
    std::string client_name;
    std::string client_uri;
    std::string server_name;
    std::string server_uri;
    int session_num = 0;
};
#endif
