#include <iostream>
#include "server.h"

int main(int argc, char* argv[]) {
    int port = 8080;
    if (argc > 1) {
        port = std::stoi(argv[1]);
    }
    initBuf();
    runServer(port);
    return 0;
}

//  g++ -g *.cc   -o main