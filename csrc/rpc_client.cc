#include "rpc_client.h"

#include <fstream>

#include "control.h"
#include "hdfs-ioengine.h"
static uint8_t flag = 0;
char dummy[] = "test";
Rpcclient rpcclient(0, clientname);
Timer timer;
FS_ENGINE fs;
void cont_func(void *_context, void *_tag) {
    flag++;
    auto resp = rpcclient.resp_msgbuf[0];
    char *buf = (char *)(resp.buf_);
    if (flag == 1) {
        printf("Query time = ");
        timer.end_print();
        printf("Result:\n %s", buf);
        rpcclient.send_req(dummy, sizeof(dummy), 0, kbereadType);
        timer.begin();
    } else if (flag == 2) {
        printf("Populate be-read time = ");
        timer.end_print();
        printf("Result:\n %s", buf);
        rpcclient.send_req(dummy, sizeof(dummy), 0, kpopularType);
        timer.begin();
    } else if (flag == 3) {
        printf("Populate popular_rank time = ");
        timer.end_print();
        printf("Result:\n %s", buf);
        if (want_file) {
            std::string commandstring;
            for (int i = 6; buf[i] != ','; ++i) commandstring.push_back(buf[i]);
            fs.LookupRecord(commandstring);
        }
    }
    return;
}
// const std::string clientname = "10.0.2.183";
// const std::string servername = "10.0.2.183";
constexpr size_t kMaxdatafilesize = 1024 * 8;
int count = 0;
int main() {
    puts("main");
    // Rpcclient rpcclient(0, clientname);
    rpcclient.Activate_client(servername);
    // std::ifstream datafile;
    // datafile.open("example.txt");
    // assert(datafile.is_open());
    // std::string line[kMaxdatafilesize];
    // int lines = 0;
    // while(std::getline(datafile, line[lines++])) {}
    // datafile.close();
    char dummy[1];
    char query[] =
        "select uid, gender, aid, title, articleTags, agreeOrNot from article join (select uid, aid, agreeOrNot, commentOrNot, shareOrNot from user_read) on "
        "aid=aid join user on uid=uid where agreeOrNot = 1 and gender = male limit 10";
    rpcclient.send_req(query, sizeof(query), count, kreadType);
    timer.begin();
    while (true) {
        rpcclient.poll();
        // if(flag == 1) {
        //     rpcclient.send_req(dummy, sizeof(dummy), count, kbereadType);
        // }
        // else if(flag == 2) {
        //     rpcclient.send_req(dummy, sizeof(dummy), count, kpopularType);
        // }
    }
}