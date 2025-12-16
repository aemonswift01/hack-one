#include <iostream>
#include "server.h"
#include "storage/graph_storage.h"

int main(int argc, char* argv[]) {
    hackathon::GraphStorage storage("graph_data");
    storage.BuildFromCSV("data/sample.csv");

    // 示例用法
    uint32_t node_id = storage.StringToId("node1");
    auto neighbors = storage.GetOutNeighbors(node_id);
    std::cout << "Node " << node_id << " has " << neighbors.size()
              << " neighbors\n";

    int port = 8080;
    if (argc > 1) {
        port = std::stoi(argv[1]);
    }
    initBuf();
    runServer(port);

    return 0;
}

//  g++ -g *.cc   -o main