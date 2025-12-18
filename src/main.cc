// src/main.cpp
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>
#include <unordered_set>
#include <queue>
#include "../include/GraphStorage.hpp"
#include "crow_all.h"

using namespace std::chrono;

// ------------------------------------------------------------
// 导入 CSV 并构建图（简化调用）
void import_graph(const std::string& csv_path, const std::string& out_dir) {
    // 实际应调用内部函数，此处为演示使用 system
    // 请确保 importer 已编译到 build/importer
    std::string cmd = "./build/importer \"" + csv_path + "\" \"" + out_dir + "\"";
    int ret = system(cmd.c_str());
    if (ret != 0) {
        throw std::runtime_error("Importer failed");
    }
}

size_t count_csv_lines(const std::string& path) {
    std::ifstream f(path);
    size_t lines = 0;
    std::string line;
    while (std::getline(f, line)) {
        if (!line.empty() && line.find_first_not_of(" \t\r\n") != std::string::npos) {
            ++lines;
        }
    }
    return lines;
}

// ------------------------------------------------------------
// 图查询服务封装
class GraphQueryService {
    GraphStorage storage_;

public:
    void load(const std::string& dir) {
        storage_.load_from_dir(dir);
    }

    // 1. k-hop neighbors count (undirected, deduplicated)
    uint64_t khop_count(std::string_view node_id, int k) {
        auto internal = storage_.external_id_to_internal(node_id);
        if (!internal) return 0;
        if (k <= 0) return 1;

        std::unordered_set<uint32_t> visited;
        std::queue<std::pair<uint32_t, int>> q;
        q.emplace(*internal, 0);
        visited.insert(*internal);

        while (!q.empty()) {
            auto [node, depth] = q.front(); q.pop();
            if (depth >= k) continue;

            auto out = storage_.out_edges(node);
            for (size_t i = 0; i < out.count; ++i) {
                if (visited.insert(out.neighbors[i]).second) {
                    q.emplace(out.neighbors[i], depth + 1);
                }
            }
            auto in = storage_.in_edges(node);
            for (size_t i = 0; i < in.count; ++i) {
                if (visited.insert(in.neighbors[i]).second) {
                    q.emplace(in.neighbors[i], depth + 1);
                }
            }
        }
        return visited.size();
    }

    // 2. common neighbors of multiple nodes
    uint64_t common_neighbors(const std::vector<std::string>& node_ids) {
        if (node_ids.empty()) return 0;
        std::vector<std::unordered_set<uint32_t>> neighbor_sets;
        for (const auto& id : node_ids) {
            auto internal = storage_.external_id_to_internal(id);
            if (!internal) return 0;
            std::unordered_set<uint32_t> nbrs;
            auto out = storage_.out_edges(*internal);
            for (size_t i = 0; i < out.count; ++i) nbrs.insert(out.neighbors[i]);
            auto in = storage_.in_edges(*internal);
            for (size_t i = 0; i < in.count; ++i) nbrs.insert(in.neighbors[i]);
            neighbor_sets.push_back(std::move(nbrs));
        }

        auto common = neighbor_sets[0];
        for (size_t i = 1; i < neighbor_sets.size(); ++i) {
            std::unordered_set<uint32_t> new_common;
            for (uint32_t n : common) {
                if (neighbor_sets[i].count(n)) {
                    new_common.insert(n);
                }
            }
            common = std::move(new_common);
        }
        return common.size();
    }

    // 3. subgraph isomorphism count
    uint64_t subgraph_count(const QueryGraph& q) {
        return storage_.count_subgraph_isomorphisms(q);
    }

    // 4. connected components (treat as undirected graph)
    uint64_t connected_components() {
        size_t n = storage_.num_nodes();
        std::vector<bool> visited(n, false);
        uint64_t comp = 0;

        for (size_t i = 0; i < n; ++i) {
            if (!visited[i]) {
                ++comp;
                std::queue<uint32_t> q;
                q.push(i);
                visited[i] = true;
                while (!q.empty()) {
                    uint32_t u = q.front(); q.pop();
                    auto out = storage_.out_edges(u);
                    for (size_t j = 0; j < out.count; ++j) {
                        if (!visited[out.neighbors[j]]) {
                            visited[out.neighbors[j]] = true;
                            q.push(out.neighbors[j]);
                        }
                    }
                    auto in = storage_.in_edges(u);
                    for (size_t j = 0; j < in.count; ++j) {
                        if (!visited[in.neighbors[j]]) {
                            visited[in.neighbors[j]] = true;
                            q.push(in.neighbors[j]);
                        }
                    }
                }
            }
        }
        return comp;
    }

    // 5. shortest path exists? (return 1 if reachable, else 0)
    uint64_t shortest_path_exists(std::string_view src_id, std::string_view dst_id) {
        auto src = storage_.external_id_to_internal(src_id);
        auto dst = storage_.external_id_to_internal(dst_id);
        if (!src || !dst) return 0;
        if (*src == *dst) return 1;

        std::queue<uint32_t> q;
        std::vector<bool> visited(storage_.num_nodes(), false);
        q.push(*src);
        visited[*src] = true;

        while (!q.empty()) {
            uint32_t u = q.front(); q.pop();
            auto out = storage_.out_edges(u);
            for (size_t i = 0; i < out.count; ++i) {
                uint32_t v = out.neighbors[i];
                if (v == *dst) return 1;
                if (!visited[v]) {
                    visited[v] = true;
                    q.push(v);
                }
            }
            auto in = storage_.in_edges(u);
            for (size_t i = 0; i < in.count; ++i) {
                uint32_t v = in.neighbors[i];
                if (v == *dst) return 1;
                if (!visited[v]) {
                    visited[v] = true;
                    q.push(v);
                }
            }
        }
        return 0;
    }
};

// ------------------------------------------------------------
int main(int argc, char* argv[]) {
    std::string csv_path;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "-f" && i + 1 < argc) {
            csv_path = argv[++i];
        } else {
            std::cerr << "Usage: " << argv[0] << " -f <csv_file>\n";
            return 1;
        }
    }
    if (csv_path.empty()) {
        std::cerr << "Error: -f required\n";
        return 1;
    }

    std::cout << "📁 Importing graph from: " << csv_path << "\n";
    auto start = steady_clock::now();

    import_graph(csv_path, "./graph_data");
    size_t lines = count_csv_lines(csv_path);

    auto end = steady_clock::now();
    double secs = duration<double>(end - start).count();

    std::cout << "✅ Import done!\n";
    std::cout << "📊 Lines: " << lines << "\n";
    std::cout << "⏱️  Time: " << secs << " s\n";
    std::cout << "🚀 Speed: " << static_cast<size_t>(lines / secs) << " rows/s\n";

    // Load into query engine
    GraphQueryService service;
    service.load("./graph_data");

    // Start REST server
    crow::SimpleApp app;

    // 1. k-hop
    CROW_ROUTE(app, "/khop")
    .methods("POST"_method)
    ([](const crow::request& req) {
        try {
            auto body = crow::json::load(req.body);
            if (!body || !body.has("node") || !body.has("k")) {
                return crow::response(400, "{\"error\":\"missing node or k\"}");
            }
            std::string node = body["node"].s();
            int k = body["k"].i();
            uint64_t cnt = service.khop_count(node, k);
            crow::json::wvalue res;
            res["count"] = cnt;
            return crow::response(crow::json::dump(res));
        } catch (const std::exception& e) {
            crow::json::wvalue err;
            err["error"] = e.what();
            return crow::response(400, crow::json::dump(err));
        }
    });

    // 2. common neighbors
    CROW_ROUTE(app, "/commNode")
    .methods("POST"_method)
    ([](const crow::request& req) {
        try {
            auto body = crow::json::load(req.body);
            if (!body.has("nodes")) {
                return crow::response(400, "{\"error\":\"missing nodes array\"}");
            }
            std::vector<std::string> nodes;
            for (auto& n : body["nodes"]) nodes.push_back(n.s());
            uint64_t cnt = service.common_neighbors(nodes);
            crow::json::wvalue res;
            res["count"] = cnt;
            return crow::response(crow::json::dump(res));
        } catch (const std::exception& e) {
            crow::json::wvalue err;
            err["error"] = e.what();
            return crow::response(400, crow::json::dump(err));
        }
    });

    // 3. subgraph
    CROW_ROUTE(app, "/subGraph")
    .methods("POST"_method)
    ([](const crow::request& req) {
        try {
            auto body = crow::json::load(req.body);
            QueryGraph q;
            for (auto& n : body["nodes"]) {
                q.nodes.push_back({static_cast<uint8_t>(n["label"].i())});
            }
            for (auto& e : body["edges"]) {
                q.edges.push_back({
                    static_cast<size_t>(e["src"].i()),
                    static_cast<size_t>(e["dst"].i()),
                    static_cast<uint8_t>(e["label"].i())
                });
            }
            uint64_t cnt = service.subgraph_count(q);
            crow::json::wvalue res;
            res["count"] = cnt;
            return crow::response(crow::json::dump(res));
        } catch (const std::exception& e) {
            crow::json::wvalue err;
            err["error"] = e.what();
            return crow::response(400, crow::json::dump(err));
        }
    });

    // 4. connected components
    CROW_ROUTE(app, "/connComp")
    .methods("POST"_method)
    ([](const crow::request& /*req*/) {
        try {
            uint64_t cnt = service.connected_components();
            crow::json::wvalue res;
            res["count"] = cnt;
            return crow::response(crow::json::dump(res));
        } catch (const std::exception& e) {
            crow::json::wvalue err;
            err["error"] = e.what();
            return crow::response(400, crow::json::dump(err));
        }
    });

    // 5. shortest path exists
    CROW_ROUTE(app, "/shortest")
    .methods("POST"_method)
    ([](const crow::request& req) {
        try {
            auto body = crow::json::load(req.body);
            if (!body.has("src") || !body.has("dst")) {
                return crow::response(400, "{\"error\":\"missing src or dst\"}");
            }
            uint64_t cnt = service.shortest_path_exists(body["src"].s(), body["dst"].s());
            crow::json::wvalue res;
            res["count"] = cnt;
            return crow::response(crow::json::dump(res));
        } catch (const std::exception& e) {
            crow::json::wvalue err;
            err["error"] = e.what();
            return crow::response(400, crow::json::dump(err));
        }
    });

    std::cout << "📡 Starting Crow REST server on http://0.0.0.0:8080\n";
    app.port(8080).multithreaded().run();
}