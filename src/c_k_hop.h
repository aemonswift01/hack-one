#ifndef KHOP_H
#define KHOP_H

#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <stdexcept>
#include "roaring.hh"
#include "entity-cc.h"

struct KHop
{
    std::vector<std::string> &ids;
    std::vector<std::string> &labels;
    uint32_t size; // 修正u_int32_t为uint32_t（标准写法）
    // 用于统计数量
    std::vector<uint64_t> counts;
    // 根据size 初始化
    std::vector<roaring::Roaring> visited;
    // 内存监控
    // 内存限制参数（单位:byte）
    size_t mem_limit;
    size_t cached_total_mem;
    size_t offset_mem;

    // 补充构造方法（仅初始化成员，不改动原有逻辑）
    KHop(std::vector<std::string> &ids_ref,
         std::vector<std::string> &labels_ref,
         uint32_t size_val);

    /**
     * @brief 计算节点数量（含string ID转uint32_t+邻接关系处理）
     * @return 有效节点处理后的总关系数量
     */
    void query();

    void k_kop_d(uint32_t id, int64_t count);

    uint64_t k_kop_b(uint32_t id, uint64_t count);

    bool check_limit0(std::vector<Link_Node> arrLinkNode0, std::vector<Link_Node> arrLinkNode1, int size);

    bool check_limit1(std::vector<Link_Node> arrLinkNode0, std::vector<Link_Node> arrLinkNode1);

    void update_vec_mem_cache(const std::vector<Link_Node> &vec);

    void update_offset_mem(Link_Node node);

private:
    bool is_over_limit(size_t mem_bytes, uint32_t limit_m) const
    {
        const size_t BYTES_PER_M = 1024 * 1024;
        size_t mem_m = mem_bytes / BYTES_PER_M;
        if (mem_bytes % BYTES_PER_M != 0)
        {
            mem_m += 1;
        }
        return mem_m > limit_m;
    }
};

#endif
