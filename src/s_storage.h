#pragma once
#include "s_other.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <filesystem>
#include <sys/stat.h>
#include <fcntl.h>
#include <lz4.h>

namespace fs = std::filesystem;

struct CACHE_ALIGN EdgeData {
    uint32_t edge_id;          // 8亿边ID（32bit）
    uint32_t node_id;          // 3亿点ID（32bit）
    uint8_t edge_label_id;     // 边标签ID（0-255）
    uint8_t is_out;            // 0=入边，1=出边
    uint8_t start_label_id;    // 起点标签ID
    uint8_t end_label_id;      // 终点标签ID
};

struct Block {
    uint32_t block_id;
    std::vector<uint32_t> offsets_out;  // 出边偏移
    std::vector<uint32_t> offsets_in;   // 入边偏移
    std::vector<EdgeData> adj_data;     // 邻接数据（出边+入边）

    Block() : offsets_out(BLOCK_SIZE + 1, 0), offsets_in(BLOCK_SIZE + 1, 0) {}
};

class BlockedCSR {
private:
    std::string cold_block_dir;

    std::string GetBlockPath(uint32_t block_id) {
        return cold_block_dir + "/block_" + std::to_string(block_id) + ".dat";
    }

public:
    BlockedCSR(const std::string& dir) : cold_block_dir(dir) {
        fs::create_directories(cold_block_dir);
    }

    void SaveBlockToDisk(const Block& block) {
        std::string path = GetBlockPath(block.block_id);
        int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) throw std::runtime_error("Create block failed: " + path);

        uint64_t adj_size = block.adj_data.size() * sizeof(EdgeData);
        uint64_t offsets_out_size = block.offsets_out.size() * sizeof(uint32_t);
        uint64_t offsets_in_size = block.offsets_in.size() * sizeof(uint32_t);
        uint64_t total_size = adj_size + offsets_out_size + offsets_in_size + sizeof(uint32_t);

        ftruncate(fd, total_size);
        void* mapped = mmap(nullptr, total_size, PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);
        if (mapped == MAP_FAILED) throw std::runtime_error("mmap write failed: " + path);

        char* ptr = static_cast<char*>(mapped);
        *(uint32_t*)ptr = block.block_id;
        ptr += sizeof(uint32_t);

        memcpy(ptr, block.offsets_out.data(), offsets_out_size);
        ptr += offsets_out_size;
        memcpy(ptr, block.offsets_in.data(), offsets_in_size);
        ptr += offsets_in_size;

        char* compressed = LZ4_compress_default((char*)block.adj_data.data(), ptr, adj_size, LZ4_compressBound(adj_size));
        (void)compressed;

        msync(mapped, total_size, MS_SYNC);
        munmap(mapped, total_size);
    }

    Block* LoadColdBlock(uint32_t block_id) {
        std::string path = GetBlockPath(block_id);
        if (!fs::exists(path)) return nullptr;

        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Open block failed: " + path);

        off_t file_size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);

        void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (mapped == MAP_FAILED) throw std::runtime_error("mmap read failed: " + path);

        char* ptr = static_cast<char*>(mapped);
        uint32_t bid = *(uint32_t*)ptr;
        ptr += sizeof(uint32_t);

        Block* block = new Block();
        block->block_id = bid;

        uint64_t offsets_out_size = BLOCK_SIZE + 1;
        memcpy(block->offsets_out.data(), ptr, offsets_out_size * sizeof(uint32_t));
        ptr += offsets_out_size * sizeof(uint32_t);

        memcpy(block->offsets_in.data(), ptr, offsets_out_size * sizeof(uint32_t));
        ptr += offsets_out_size * sizeof(uint32_t);

        uint64_t adj_size = file_size - (ptr - static_cast<char*>(mapped));
        block->adj_data.resize(adj_size / sizeof(EdgeData));
        LZ4_decompress_safe(ptr, (char*)block->adj_data.data(), adj_size, adj_size);

        munmap(mapped, file_size);
        return block;
    }

    void AddBlock(const Block& block, bool is_hot) {
        if (!is_hot) SaveBlockToDisk(block);
    }
};