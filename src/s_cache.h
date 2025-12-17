#pragma once
#include "s_storage.h"
#include "s_other.h"
#include <unordered_map>
#include <atomic>

class CacheManager {
private:
    std::unordered_map<uint32_t, Block*> hot_blocks;
    BlockedCSR* csr;
    MemoryPoolManager* mem_pool;
    uint64_t max_hot_mem = 1ULL * 1024 * 1024 * 1024;  // 1G热块缓存
    std::atomic<uint64_t> current_hot_mem = 0;

    uint64_t CalcBlockMem(const Block& block) {
        return block.adj_data.size() * sizeof(EdgeData) +
               block.offsets_out.size() * sizeof(uint32_t) +
               block.offsets_in.size() * sizeof(uint32_t);
    }

    void EvictColdBlock() {
        if (hot_blocks.empty()) return;
        auto it = hot_blocks.begin();
        current_hot_mem -= CalcBlockMem(*it->second);
        delete it->second;
        hot_blocks.erase(it);
    }

public:
    CacheManager(BlockedCSR* c, MemoryPoolManager* mp) : csr(c), mem_pool(mp) {}

    ~CacheManager() {
        for (auto& [_, block] : hot_blocks) delete block;
    }

    Block* GetBlock(uint32_t block_id) {
        if (hot_blocks.count(block_id)) return hot_blocks[block_id];
        Block* block = csr->LoadColdBlock(block_id);
        if (!block) {
            block = new Block();
            block->block_id = block_id;
        }
        current_hot_mem += CalcBlockMem(*block);
        while (current_hot_mem > max_hot_mem) EvictColdBlock();
        hot_blocks[block_id] = block;
        return block;
    }

    void AddHotBlock(const Block& block) {
        uint64_t mem = CalcBlockMem(block);
        while (current_hot_mem + mem > max_hot_mem) EvictColdBlock();

        Block* new_block = new Block(block);
        hot_blocks[block.block_id] = new_block;
        current_hot_mem += mem;
    }

    void ShrinkHotCache(float ratio) {
        uint64_t target = max_hot_mem * ratio;
        while (current_hot_mem > target) EvictColdBlock();
    }

    void ReleaseImportTempMem() {
        for (auto& [_, block] : hot_blocks) {
            if (block->block_id >= 1000) {
                block->adj_data.clear();
                block->adj_data.shrink_to_fit();
            }
        }
        current_hot_mem = 0;
        for (auto& [_, block] : hot_blocks) current_hot_mem += CalcBlockMem(*block);
        
        malloc_trim(0);
        madvise(0, 0, MADV_PAGEOUT);
    }

    uint64_t GetCurrentHotMem() { return current_hot_mem; }
};