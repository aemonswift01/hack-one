#pragma once
#include <cstdint>
#include <cstdlib>
#include <sys/mman.h>
#include <stdexcept>
#include <immintrin.h>
#include <absl/container/flat_hash_map.h>
#include <atomic>

constexpr uint32_t BLOCK_SIZE = 65536;
constexpr uint64_t EMERGENCY_POOL_SIZE = 500ULL * 1024 * 1024;
constexpr uint64_t TOTAL_MEM_LIMIT = 6ULL * 1024 * 1024 * 1024;
constexpr uint16_t INVALID_LABEL_ID = 0;

constexpr int SIMD_WIDTH = 8;
using SIMDInt32 = __m512i;

constexpr size_t CACHE_LINE_SIZE = 128;
#define CACHE_ALIGN __attribute__((aligned(CACHE_LINE_SIZE)))

struct MemoryPoolManager {
    void* emergency_pool = nullptr;
    void* subgraph_pool = nullptr;
    void* query_pool = nullptr;
    uint64_t subgraph_pool_size = 0;
    uint64_t query_pool_size = 0;

    MemoryPoolManager() {
        emergency_pool = mmap(nullptr, EMERGENCY_POOL_SIZE,
                              PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                              -1, 0);
        if (emergency_pool == MAP_FAILED) {
            throw std::runtime_error("Failed to allocate emergency pool");
        }
    }

    ~MemoryPoolManager() {
        if (emergency_pool) munmap(emergency_pool, EMERGENCY_POOL_SIZE);
        if (subgraph_pool && subgraph_pool != emergency_pool) munmap(subgraph_pool, subgraph_pool_size);
        if (query_pool && query_pool != emergency_pool) munmap(query_pool, query_pool_size);
    }

    void FreeSubgraphPool() {
        if (subgraph_pool) {
            munlock(subgraph_pool, subgraph_pool_size);
            subgraph_pool_size = 0;
            subgraph_pool = nullptr;
        }
    }

    bool AllocQueryPool(uint64_t query_mem_mb) {
        uint64_t total_need = query_mem_mb * 1024 * 1024;
        if (total_need <= EMERGENCY_POOL_SIZE) {
            query_pool = emergency_pool;
            mlock(emergency_pool, EMERGENCY_POOL_SIZE);
            query_pool_size = EMERGENCY_POOL_SIZE;
            return true;
        }

        uint64_t extra_size = total_need - EMERGENCY_POOL_SIZE;
        void* extra_pool = mmap(nullptr, extra_size, PROT_READ | PROT_WRITE,
                                MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (extra_pool == MAP_FAILED) {
            query_pool = emergency_pool;
            query_pool_size = EMERGENCY_POOL_SIZE;
            return false;
        }
        query_pool = emergency_pool;
        query_pool_size = total_need;
        mlock(emergency_pool, EMERGENCY_POOL_SIZE);
        mlock(extra_pool, extra_size);
        return true;
    }
};

inline uint64_t GetUsedMemory() {
    std::ifstream stat("/proc/self/statm");
    uint64_t size, resident;
    stat >> size >> resident;
    return resident * 4096;
}

inline bool SetProcessMemLimit(uint64_t mem_limit_mb) {
    struct rlimit rl;
    rl.rlim_cur = mem_limit_mb * 1024 * 1024;
    rl.rlim_max = mem_limit_mb * 1024 * 1024;
    return setrlimit(RLIMIT_AS, &rl) == 0;
}