#include "server.h"
#include <assert.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "itoa.h"

// 简单的 HTTP 请求解析结构
struct HttpRequest {
    // std::string_view method;
    // int method_;
    std::string_view path;
    std::string_view body;
};

constexpr int a = 10;

char resbuf[128];

int len;

void initBuf() {
    char* format =
        "HTTP/1.1 200 OK\r\nContent-Type: "
        "application/json\r\n\r\n{\"count\":";
    int lg = 0;
    while (format[lg++])
        ;
    memcpy(resbuf, format, lg);
    len = lg - 1;
}

// 简易 HTTP 请求解析器（仅处理单次 recv 的完整请求）
HttpRequest parseHttpRequest(const std::string_view raw) {
    HttpRequest req;
    auto end = raw.find(" ");
    req.path = raw.substr(0, end);
    auto start = end + 11;
    auto dataIndex = raw.substr(start).find("\r\n\r\n");  // " HTTP/1.1\r\n"
    req.body = raw.substr(start + dataIndex);
    return req;
}

int makeResponse(int count) {
    char temp[8]{};
    itoa_fwd(count, temp);
    int lg = 0;
    while (temp[lg++])
        ;
    memcpy(resbuf + len, temp, lg - 1);
    resbuf[len + lg - 1] = '}';

    return len + lg - 1;
}

// 处理单个请求
int handleRequest(const HttpRequest& req) {
    //TODO
    return makeResponse(11);
}

// 主服务器循环
void runServer(int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("socket");
        return;
    }

    // 允许端口复用
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("bind");
        close(server_fd);
        return;
    }

    if (listen(server_fd, 5) < 0) {
        perror("listen");
        close(server_fd);
        return;
    }

    std::cout << "HTTP server listening on port " << port << " ...\n";

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd =
            accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        // 接收请求（简单：一次 recv，假设请求完整）
        char buffer[1024];
        ssize_t bytes = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes <= 0) {
            close(client_fd);
            continue;
        }
        buffer[bytes] = '\0';

        std::string_view requestStr(buffer + 5, buffer + bytes);  //POST离开
        HttpRequest req = parseHttpRequest(requestStr);
        auto responseLen = handleRequest(req);
        // resbuf[responseLen + 1] = '\0';
        // std::cout << responseLen << resbuf[responseLen]
        //           << resbuf[responseLen - 1] << resbuf[responseLen - 2]
        //           << resbuf[responseLen - 2] << std::endl;

        std::cout << responseLen << (char*)resbuf << std::endl;

        // 发送响应
        send(client_fd, resbuf, responseLen + 1, 0);
        close(client_fd);
    }

    close(server_fd);
}

// ===== 主函数 =====