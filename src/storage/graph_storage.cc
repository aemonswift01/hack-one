// src/storage/graph_storage.cc
#include "graph_storage.h"
#include <cmath>
#include <cstring>
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

void GraphStorage::MapFile(const std::string& path, CSR& csr,
                           bool read_only) const {
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

void GraphStorage::UnmapFile(CSR& csr) const {
    if (csr.is_mapped) {
        munmap(csr.data, csr.size);
        close(csr.fd);
        csr.is_mapped = false;
        csr.data = nullptr;
        csr.fd = -1;
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

GraphStorage::GraphStorage(const std::string& base_dir) {
    // 初始化 CSR 结构
    forward_offsets_ = {-1, nullptr, 0, false};
    forward_neighbors_ = {-1, nullptr, 0, false};
    backward_offsets_ = {-1, nullptr, 0, false};
    backward_neighbors_ = {-1, nullptr, 0, false};

    std::string forward_offsets_path = base_dir + "/forward_offsets.bin";
    std::string forward_neighbors_path = base_dir + "/forward_neighbors.bin";
    std::string backward_offsets_path = base_dir + "/backward_offsets.bin";
    std::string backward_neighbors_path = base_dir + "/backward_neighbors.bin";
    std::string id_to_str_path = base_dir + "/id_to_str.bin";

    if (std::filesystem::exists(forward_offsets_path)) {
        const_cast<GraphStorage*>(this)->MapFile(forward_offsets_path,
                                                 forward_offsets_, true);
        const_cast<GraphStorage*>(this)->MapFile(forward_neighbors_path,
                                                 forward_neighbors_, true);

        // 流式加载节点映射
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

void GraphStorage::BuildFromCSV(const std::string& csv_path) {
    std::string base_dir =
        std::filesystem::path(csv_path).parent_path().string() + "/graph_data";
    std::filesystem::create_directories(base_dir);

    // 第一步：收集所有节点字符串（使用临时文件）
    std::string nodes_temp = base_dir + "/nodes_temp.bin";
    std::ofstream nodes_out(nodes_temp, std::ios::binary);

    std::ifstream csv_file(csv_path);
    if (!csv_file)
        throw std::runtime_error("Failed to open CSV file");

    std::string line;
    uint64_t edge_count = 0;

    // 第一遍：收集节点
    while (std::getline(csv_file, line)) {
        std::istringstream iss(line);
        std::string start_id, end_id;
        if (std::getline(iss, start_id, ',') &&
            std::getline(iss, end_id, ',') &&
            std::getline(iss, start_id, ',') &&
            std::getline(iss, end_id, ',')) {

            // 写入起始节点
            uint32_t len = start_id.size();
            nodes_out.write(reinterpret_cast<const char*>(&len), sizeof(len));
            nodes_out.write(start_id.c_str(), len);

            // 写入结束节点
            len = end_id.size();
            nodes_out.write(reinterpret_cast<const char*>(&len), sizeof(len));
            nodes_out.write(end_id.c_str(), len);

            edge_count++;
        }
    }
    nodes_out.close();

    // 第二步：排序和去重节点
    std::string nodes_sorted = base_dir + "/nodes_sorted.bin";
    std::string sort_cmd = "sort -u " + nodes_temp + " -o " + nodes_sorted;
    if (system(sort_cmd.c_str()) != 0) {
        throw std::runtime_error("Failed to sort nodes");
    }

    // 第三步：构建节点映射
    std::ifstream nodes_in(nodes_sorted, std::ios::binary);
    std::unordered_map<std::string, uint32_t> str_to_id;
    std::vector<std::string> id_to_str;

    uint32_t node_id = 0;
    while (true) {
        uint32_t len;
        nodes_in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (nodes_in.eof())
            break;

        std::string str(len, '\0');
        nodes_in.read(&str[0], len);
        str_to_id[str] = node_id;
        id_to_str.push_back(str);
        node_id++;
    }
    node_count_ = node_id;
    edge_count_ = edge_count;

    // 第四步：处理边（流式处理）
    std::string edges_temp = base_dir + "/edges_temp.bin";
    std::ofstream edges_out(edges_temp, std::ios::binary);

    csv_file.clear();
    csv_file.seekg(0);

    while (std::getline(csv_file, line)) {
        std::istringstream iss(line);
        std::string start_id, end_id;
        if (std::getline(iss, start_id, ',') &&
            std::getline(iss, end_id, ',') &&
            std::getline(iss, start_id, ',') &&
            std::getline(iss, end_id, ',')) {

            uint32_t src = str_to_id[start_id];
            uint32_t dst = str_to_id[end_id];

            // 写入边
            edges_out.write(reinterpret_cast<const char*>(&src), sizeof(src));
            edges_out.write(reinterpret_cast<const char*>(&dst), sizeof(dst));
        }
    }
    edges_out.close();

    // 第五步：构建正向 CSR（使用外部排序）
    std::string edges_sorted = base_dir + "/edges_sorted.bin";
    sort_cmd = "sort -k1,1 -k2,2 " + edges_temp + " -o " + edges_sorted;
    if (system(sort_cmd.c_str()) != 0) {
        throw std::runtime_error("Failed to sort edges");
    }

    // 计算 offsets
    std::vector<uint32_t> forward_offsets(node_count_ + 1, 0);
    std::ifstream edges_sorted_in(edges_sorted, std::ios::binary);

    uint32_t current_src = 0;
    uint32_t count = 0;
    while (true) {
        uint32_t src, dst;
        edges_sorted_in.read(reinterpret_cast<char*>(&src), sizeof(src));
        if (edges_sorted_in.eof())
            break;
        edges_sorted_in.read(reinterpret_cast<char*>(&dst), sizeof(dst));

        while (current_src < src) {
            forward_offsets[current_src + 1] = count;
            current_src++;
        }
        count++;
    }
    while (current_src <= node_count_) {
        forward_offsets[current_src + 1] = count;
        current_src++;
    }

    // 第六步：构建压缩的 neighbors
    std::vector<uint8_t> forward_neighbors_data;
    edges_sorted_in.clear();
    edges_sorted_in.seekg(0);

    uint32_t prev_src = 0;
    std::vector<uint32_t> current_neighbors;

    while (true) {
        uint32_t src, dst;
        edges_sorted_in.read(reinterpret_cast<char*>(&src), sizeof(src));
        if (edges_sorted_in.eof())
            break;
        edges_sorted_in.read(reinterpret_cast<char*>(&dst), sizeof(dst));

        if (src != prev_src) {
            if (!current_neighbors.empty()) {
                auto compressed = CompressNeighbors(current_neighbors);
                forward_neighbors_data.insert(forward_neighbors_data.end(),
                                              compressed.begin(),
                                              compressed.end());
                current_neighbors.clear();
            }
            prev_src = src;
        }
        current_neighbors.push_back(dst);
    }
    if (!current_neighbors.empty()) {
        auto compressed = CompressNeighbors(current_neighbors);
        forward_neighbors_data.insert(forward_neighbors_data.end(),
                                      compressed.begin(), compressed.end());
    }

    // 第七步：保存数据
    WriteBinaryFile(base_dir + "/forward_offsets.bin", forward_offsets.data(),
                    forward_offsets.size() * sizeof(uint32_t));
    WriteBinaryFile(base_dir + "/forward_neighbors.bin",
                    forward_neighbors_data.data(),
                    forward_neighbors_data.size());

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

    // 清理临时文件
    std::remove(nodes_temp.c_str());
    std::remove(edges_temp.c_str());
    std::remove(edges_sorted.c_str());
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

    // 按需加载反向 CSR
    if (!backward_offsets_.is_mapped) {
        std::string base_dir = std::filesystem::path("graph_data").string();
        const_cast<GraphStorage*>(this)->MapFile(
            base_dir + "/backward_offsets.bin",
            const_cast<CSR&>(backward_offsets_), true);
        const_cast<GraphStorage*>(this)->MapFile(
            base_dir + "/backward_neighbors.bin",
            const_cast<CSR&>(backward_neighbors_), true);
    }

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
