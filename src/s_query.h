#pragma once
#include "s_cache.h"
#include "iterator.h"
#include <vector>

class AdjQueryIterator : public AdjIterator {
private:
    Block* block;
    uint32_t start_idx;
    uint32_t end_idx;
    uint32_t current_idx;
    bool is_out;

public:
    AdjQueryIterator(Block* b, uint32_t local_id, bool out) : block(b), is_out(out) {
        if (is_out) {
            start_idx = block->offsets_out[local_id];
            end_idx = block->offsets_out[local_id + 1];
        } else {
            start_idx = block->offsets_in[local_id];
            end_idx = block->offsets_in[local_id + 1];
        }
        current_idx = start_idx;
    }

    bool HasNext() const override { return current_idx < end_idx; }

    void Next() override { current_idx++; }

    void Reset() override { current_idx = start_idx; }

    const std::pair<uint32_t, uint32_t>* GetValuePtr() const override {
        static std::pair<uint32_t, uint32_t> res;
        const EdgeData& edge = block->adj_data[current_idx];
        res.first = edge.edge_id;
        res.second = edge.node_id;
        return &res;
    }
};

class QueryManager {
private:
    BlockedCSR* csr;
    CacheManager* cache;
    MemoryPoolManager* mem_pool;

public:
    QueryManager(BlockedCSR* c, CacheManager* cm, MemoryPoolManager* mp) : csr(c), cache(cm), mem_pool(mp) {}

    AdjIterator* GetAdjIterator(uint32_t point_id, bool is_out) {
        uint32_t block_id = point_id / BLOCK_SIZE;
        uint32_t local_id = point_id % BLOCK_SIZE;
        Block* block = cache->GetBlock(block_id);
        return new AdjQueryIterator(block, local_id, is_out);
    }

    std::vector<uint32_t> GetNeighbors(uint32_t point_id, bool is_out) {
        std::vector<uint32_t> res;
        AdjIterator* iter = GetAdjIterator(point_id, is_out);
        while (iter->HasNext()) {
            res.push_back(iter->GetValuePtr()->second);
            iter->Next();
        }
        delete iter;
        return res;
    }

    bool AllocQueryMem(uint64_t mem_mb) {
        return mem_pool->AllocQueryPool(mem_mb);
    }
};