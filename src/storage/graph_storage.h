// src/storage/graph_storage.h
#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace hackathon {

class GraphStorage {
   public:
    GraphStorage(const std::string& base_dir);
    ~GraphStorage();

    void BuildFromCSV(const std::string& csv_path);
    uint32_t OutDegree(uint32_t node_id) const;
    uint32_t InDegree(uint32_t node_id) const;
    std::vector<uint32_t> GetOutNeighbors(uint32_t node_id) const;
    std::vector<uint32_t> GetInNeighbors(uint32_t node_id) const;
    uint32_t StringToId(const std::string& str_id) const;
    std::string IdToString(uint32_t id) const;

    uint32_t NodeCount() const { return node_count_; }

    uint64_t EdgeCount() const { return edge_count_; }

   private:
    struct CSR {
        int fd;
        uint8_t* data;
        size_t size;
        bool is_mapped;
    };

    std::unordered_map<std::string, uint32_t> str_to_id_;
    std::vector<std::string> id_to_str_;
    mutable CSR forward_offsets_;
    mutable CSR forward_neighbors_;
    mutable CSR backward_offsets_;
    mutable CSR backward_neighbors_;
    uint32_t node_count_ = 0;
    uint64_t edge_count_ = 0;

    void MapFile(const std::string& path, CSR& csr,
                 bool read_only = true) const;
    void UnmapFile(CSR& csr) const;
    static std::vector<uint8_t> CompressNeighbors(
        const std::vector<uint32_t>& neighbors);
    static std::vector<uint32_t> DecompressNeighbors(const uint8_t* data,
                                                     size_t size);
    void WriteBinaryFile(const std::string& path, const void* data,
                         size_t size);
    std::vector<uint8_t> ReadBinaryFile(const std::string& path);
};

}  // namespace hackathon
