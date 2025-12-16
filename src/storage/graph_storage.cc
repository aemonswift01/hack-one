// src/storage/graph_storage.cc
#include "graph_storage.h"
#include <cmath>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <queue>
#include <set>
#include <sstream>

namespace hackathon {

std::vector<uint8_t> EncodeVarint(uint32_t value) {
    std::vector<uint8_t> result;
    while (value > 0x7F) {
        result.push_back((value & 0x7F) | 0x80);
        value >>= 7;
    }
    result.push_back(value);
    return result;
}

uint32_t DecodeVarint(const uint8_t*& data) {
    uint32_t value = 0;
    int shift = 0;
    while (true) {
        uint8_t byte = *data++;
        value |= (byte & 0x7F) << shift;
        if ((byte & 0x80) == 0)
            break;
        shift += 7;
    }
    return value;
}

GraphStorage::GraphStorage(const std::string& base_dir) {
    std::filesystem::create_directories(base_dir);
    std::string forward_offsets_path = base_dir + "/forward_offsets.bin";
    std::string forward_neighbors_path = base_dir + "/forward_neighbors.bin";
    std::string backward_offsets_path = base_dir + "/backward_offsets.bin";
    std::string backward_neighbors_path = base_dir + "/backward_neighbors.bin";
    std::string id_to_str_path = base_dir + "/id_to_str.bin";

    if (std::filesystem::exists(forward_offsets_path)) {
        MapFile(forward_offsets_path, forward_offsets_, true);
        MapFile(forward_neighbors_path, forward_neighbors_, true);
        MapFile(backward_offsets_path, backward_offsets_, true);
        MapFile(backward_neighbors_path, backward_neighbors_, true);

        std::ifstream id_file(id_to_str_path, std::ios::binary);
        if (id_file) {
            uint32_t count;
            id_file.read(reinterpret_cast<char*>(&count), sizeof(count));
            id_to_str_.resize(count);
            for (uint32_t i = 0; i < count; ++i) {
                uint32_t len;
                id_file.read(reinterpret_cast<char*>(&len), sizeof(len));
                std::string str(len, '\0');
                id_file.read(&str[0], len);
                id_to_str_[i] = str;
                str_to_id_[str] = i;
            }
            node_count_ = count;
        }
    }
}

GraphStorage::~GraphStorage() {
    UnmapFile(forward_offsets_);
    UnmapFile(forward_neighbors_);
    UnmapFile(backward_offsets_);
    UnmapFile(backward_neighbors_);
}

void GraphStorage::MapFile(const std::string& path, CSR& csr, bool read_only) {
    int fd = open(path.c_str(), read_only ? O_RDONLY : O_RDWR);
    if (fd == -1)
        throw std::runtime_error("Failed to open file: " + path);

    struct stat st;
    if (fstat(fd, &st) == -1) {
        close(fd);
        throw std::runtime_error("Failed to stat file: " + path);
    }

    void* data =
        mmap(nullptr, st.st_size,
             read_only ? PROT_READ : PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("Failed to mmap file: " + path);
    }

    csr.fd = fd;
    csr.data = static_cast<uint8_t*>(data);
    csr.size = st.st_size;
    csr.is_mapped = true;
}

void GraphStorage::UnmapFile(CSR& csr) {
    if (csr.is_mapped) {
        munmap(csr.data, csr.size);
        close(csr.fd);
        csr.is_mapped = false;
    }
}

std::vector<uint8_t> GraphStorage::CompressNeighbors(
    const std::vector<uint32_t>& neighbors) {
    if (neighbors.empty())
        return {};

    std::vector<uint8_t> result;
    uint32_t prev = 0;
    for (uint32_t val : neighbors) {
        uint32_t delta = val - prev;
        auto encoded = EncodeVarint(delta);
        result.insert(result.end(), encoded.begin(), encoded.end());
        prev = val;
    }
    return result;
}

std::vector<uint32_t> GraphStorage::DecompressNeighbors(const uint8_t* data,
                                                        size_t size) {
    std::vector<uint32_t> result;
    const uint8_t* ptr = data;
    const uint8_t* end = data + size;
    uint32_t prev = 0;

    while (ptr < end) {
        uint32_t delta = DecodeVarint(ptr);
        prev += delta;
        result.push_back(prev);
    }
    return result;
}

void GraphStorage::WriteBinaryFile(const std::string& path, const void* data,
                                   size_t size) {
    std::ofstream out(path, std::ios::binary);
    out.write(static_cast<const char*>(data), size);
}

std::vector<uint8_t> GraphStorage::ReadBinaryFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in)
        throw std::runtime_error("Failed to open file: " + path);

    size_t size = in.tellg();
    in.seekg(0);
    std::vector<uint8_t> data(size);
    in.read(reinterpret_cast<char*>(data.data()), size);
    return data;
}

void GraphStorage::BuildFromCSV(const std::string& csv_path) {
    // 第一步：收集所有节点字符串
    std::unordered_map<std::string, uint32_t> str_to_id;
    std::vector<std::string> id_to_str;
    std::unordered_map<std::string, uint32_t> str_to_id_temp;
    std::vector<std::string> all_nodes;

    std::ifstream csv_file(csv_path);
    if (!csv_file)
        throw std::runtime_error("Failed to open CSV file");

    std::string line;
    while (std::getline(csv_file, line)) {
        std::istringstream iss(line);
        std::string start_id, start_label, edge_label, end_id, end_label;
        if (std::getline(iss, start_id, ',') &&
            std::getline(iss, start_label, ',') &&
            std::getline(iss, edge_label, ',') &&
            std::getline(iss, end_id, ',') && std::getline(iss, end_label)) {

            if (str_to_id_temp.find(start_id) == str_to_id_temp.end()) {
                str_to_id_temp[start_id] = all_nodes.size();
                all_nodes.push_back(start_id);
            }
            if (str_to_id_temp.find(end_id) == str_to_id_temp.end()) {
                str_to_id_temp[end_id] = all_nodes.size();
                all_nodes.push_back(end_id);
            }
        }
    }

    // 分配整数 ID
    node_count_ = all_nodes.size();
    for (uint32_t i = 0; i < node_count_; ++i) {
        str_to_id[all_nodes[i]] = i;
        id_to_str.push_back(all_nodes[i]);
    }

    // 第二步：处理边
    std::vector<std::pair<uint32_t, uint32_t>> edges;
    csv_file.clear();
    csv_file.seekg(0);

    while (std::getline(csv_file, line)) {
        std::istringstream iss(line);
        std::string start_id, start_label, edge_label, end_id, end_label;
        if (std::getline(iss, start_id, ',') &&
            std::getline(iss, start_label, ',') &&
            std::getline(iss, edge_label, ',') &&
            std::getline(iss, end_id, ',') && std::getline(iss, end_label)) {

            uint32_t src = str_to_id[start_id];
            uint32_t dst = str_to_id[end_id];
            edges.emplace_back(src, dst);
        }
    }

    edge_count_ = edges.size();

    // 第三步：构建正向 CSR
    std::vector<std::vector<uint32_t>> forward_adj(node_count_);
    for (const auto& edge : edges) {
        forward_adj[edge.first].push_back(edge.second);
    }

    // 排序邻居列表
    for (auto& neighbors : forward_adj) {
        std::sort(neighbors.begin(), neighbors.end());
    }

    // 计算 offsets
    std::vector<uint32_t> forward_offsets(node_count_ + 1, 0);
    for (uint32_t i = 0; i < node_count_; ++i) {
        forward_offsets[i + 1] = forward_offsets[i] + forward_adj[i].size();
    }

    // 压缩 neighbors
    std::vector<uint8_t> forward_neighbors_data;
    for (const auto& neighbors : forward_adj) {
        auto compressed = CompressNeighbors(neighbors);
        forward_neighbors_data.insert(forward_neighbors_data.end(),
                                      compressed.begin(), compressed.end());
    }

    // 第四步：构建反向 CSR
    std::vector<std::vector<uint32_t>> backward_adj(node_count_);
    for (const auto& edge : edges) {
        backward_adj[edge.second].push_back(edge.first);
    }

    for (auto& neighbors : backward_adj) {
        std::sort(neighbors.begin(), neighbors.end());
    }

    std::vector<uint32_t> backward_offsets(node_count_ + 1, 0);
    for (uint32_t i = 0; i < node_count_; ++i) {
        backward_offsets[i + 1] = backward_offsets[i] + backward_adj[i].size();
    }

    std::vector<uint8_t> backward_neighbors_data;
    for (const auto& neighbors : backward_adj) {
        auto compressed = CompressNeighbors(neighbors);
        backward_neighbors_data.insert(backward_neighbors_data.end(),
                                       compressed.begin(), compressed.end());
    }

    // 第五步：保存数据
    std::string base_dir =
        std::filesystem::path(csv_path).parent_path().string() + "/graph_data";
    std::filesystem::create_directories(base_dir);

    WriteBinaryFile(base_dir + "/forward_offsets.bin", forward_offsets.data(),
                    forward_offsets.size() * sizeof(uint32_t));
    WriteBinaryFile(base_dir + "/forward_neighbors.bin",
                    forward_neighbors_data.data(),
                    forward_neighbors_data.size());
    WriteBinaryFile(base_dir + "/backward_offsets.bin", backward_offsets.data(),
                    backward_offsets.size() * sizeof(uint32_t));
    WriteBinaryFile(base_dir + "/backward_neighbors.bin",
                    backward_neighbors_data.data(),
                    backward_neighbors_data.size());

    // 保存节点映射
    std::ofstream id_file(base_dir + "/id_to_str.bin", std::ios::binary);
    uint32_t count = id_to_str.size();
    id_file.write(reinterpret_cast<const char*>(&count), sizeof(count));
    for (const auto& str : id_to_str) {
        uint32_t len = str.size();
        id_file.write(reinterpret_cast<const char*>(&len), sizeof(len));
        id_file.write(str.c_str(), len);
    }

    // 更新内部状态
    str_to_id_ = std::move(str_to_id);
    id_to_str_ = std::move(id_to_str);
}

uint32_t GraphStorage::OutDegree(uint32_t node_id) const {
    if (node_id >= node_count_)
        return 0;
    const uint32_t* offsets =
        reinterpret_cast<const uint32_t*>(forward_offsets_.data);
    return offsets[node_id + 1] - offsets[node_id];
}

uint32_t GraphStorage::InDegree(uint32_t node_id) const {
    if (node_id >= node_count_)
        return 0;
    const uint32_t* offsets =
        reinterpret_cast<const uint32_t*>(backward_offsets_.data);
    return offsets[node_id + 1] - offsets[node_id];
}

std::vector<uint32_t> GraphStorage::GetOutNeighbors(uint32_t node_id) const {
    if (node_id >= node_count_)
        return {};

    const uint32_t* offsets =
        reinterpret_cast<const uint32_t*>(forward_offsets_.data);
    uint32_t start = offsets[node_id];
    uint32_t end = offsets[node_id + 1];

    const uint8_t* neighbors_data = forward_neighbors_.data + start;
    size_t size = end - start;

    return DecompressNeighbors(neighbors_data, size);
}

std::vector<uint32_t> GraphStorage::GetInNeighbors(uint32_t node_id) const {
    if (node_id >= node_count_)
        return {};

    const uint32_t* offsets =
        reinterpret_cast<const uint32_t*>(backward_offsets_.data);
    uint32_t start = offsets[node_id];
    uint32_t end = offsets[node_id + 1];

    const uint8_t* neighbors_data = backward_neighbors_.data + start;
    size_t size = end - start;

    return DecompressNeighbors(neighbors_data, size);
}

uint32_t GraphStorage::StringToId(const std::string& str_id) const {
    auto it = str_to_id_.find(str_id);
    if (it == str_to_id_.end())
        return static_cast<uint32_t>(-1);
    return it->second;
}

std::string GraphStorage::IdToString(uint32_t id) const {
    if (id >= id_to_str_.size())
        return "";
    return id_to_str_[id];
}

}  // namespace hackathon
