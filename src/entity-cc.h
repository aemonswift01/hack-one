#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <stdexcept>
#include "roaring.hh"
#include <unordered_set>

// 定义 rel 结构体
struct Rel
{
    int32_t rid;      // 32位整数ID
    std::string name; // 关联节点名称
};

// 前置声明：邻接迭代器元素类型（模拟迭代返回的rel+otherId）
struct Adjacent
{
    Rel rel;          // 关联关系信息
    uint32_t otherId; // 邻接节点ID
};

// 定义 link_node 结构体
// 提供update_node(uint32 nodeid) 实现更新endid
// 提供bool add_rel(uint32 nodeid) 判断存在性，存在返回false，不存在则加入并返回true
struct alignas(8) Link_Node
{
    roaring::Roaring rel_ids;
    uint32_t endid; // 目标节点ID

    // 更新endid
    void update_node(uint32_t nodeid)
    {
        endid = nodeid;
    }

    // 此处添加exist方法
    bool exist(uint32_t rel_id) const
    {
        return rel_ids.contains(rel_id);
    }

    void add_rel(uint32_t rel_id)
    {
        rel_ids.add(rel_id); // 不存在则加入
    }

    size_t mem() const
    {
        // 结构体自身内存（含对齐） + Roaring位图动态数据内存
        return sizeof(Link_Node) + (rel_ids.getSizeInBytes() - sizeof(roaring::Roaring));
    }

    Link_Node clone() const
    {
        Link_Node new_node;
        new_node.endid = this->endid;
        new_node.rel_ids = this->rel_ids;
        return new_node;
    }
};

struct DSU
{
    std::vector<size_t> parent; // 父节点数组
    std::vector<size_t> rank;   // 秩（用于按秩合并，加速查找）

    // 初始化：每个元素的父节点是自己
    DSU(size_t n)
    {
        parent.resize(n);
        rank.resize(n, 0);
        std::iota(parent.begin(), parent.end(), 0); // 0,1,2,...,n-1
    }

    // 查找根节点（路径压缩，极致加速）
    size_t find(size_t x)
    {
        if (parent[x] != x)
        {
            parent[x] = find(parent[x]); // 路径压缩：直接指向根节点
        }
        return parent[x];
    }

    // 合并两个元素（按秩合并，避免树退化成链表）
    void unite(size_t x, size_t y)
    {
        size_t rx = find(x);
        size_t ry = find(y);
        if (rx == ry)
            return; // 已在同一集合，无需合并

        // 按秩合并：小秩合并到大秩下
        if (rank[rx] < rank[ry])
        {
            parent[rx] = ry;
        }
        else
        {
            parent[ry] = rx;
            if (rank[rx] == rank[ry])
            {
                rank[rx]++;
            }
        }
    }
};

// ========== 极致优化的DSU（减少函数调用/内存访问） ==========
struct FastDSU
{
    size_t *parent; // 裸指针替代vector，减少内存间接访问
    size_t *rank;
    size_t size;

    // 构造：裸指针+预分配，避免vector的初始化开销
    FastDSU(size_t n) : size(n)
    {
        parent = new size_t[n];
        rank = new size_t[n](); // 初始化为0
        // 手动填充parent（比iota快，减少函数调用）
        for (size_t i = 0; i < n; ++i)
            parent[i] = i;
    }

    // 析构：手动释放内存
    ~FastDSU()
    {
        delete[] parent;
        delete[] rank;
    }

    // 查找：内联+路径压缩（避免函数调用开销）
    inline size_t find(size_t x) noexcept
    {
        // 路径压缩：迭代版（比递归快，避免栈开销）
        while (parent[x] != x)
        {
            parent[x] = parent[parent[x]]; // 路径压缩（隔代指向）
            x = parent[x];
        }
        return x;
    }

    // 合并：内联+按秩合并（无冗余判断）
    inline void unite(size_t x, size_t y) noexcept
    {
        size_t rx = find(x);
        size_t ry = find(y);
        if (rx == ry)
            return;

        // 按秩合并：极简逻辑
        if (rank[rx] < rank[ry])
        {
            parent[rx] = ry;
        }
        else
        {
            parent[ry] = rx;
            if (rank[rx] == rank[ry])
                rank[rx]++;
        }
    }

    // 禁用拷贝/移动（避免内存错误）
    FastDSU(const FastDSU &) = delete;
    FastDSU &operator=(const FastDSU &) = delete;
};

// 返回交集基数
inline bool has_intersection(const roaring::Roaring &a, const roaring::Roaring &b) noexcept
{
    if (a.cardinality() == 0 || b.cardinality() == 0)
        return false;
    return a.and_cardinality(b) > 0;
}

/**
 * @brief 模拟批量转换节点ID（string → uint32_t）
 * @param ids 字符串类型的节点ID列表
 * @return 转换后的uint32_t类型ID列表
 */
inline std::vector<uint32_t> batch_convert_node_id(const std::vector<std::string> &ids)
{
    std::vector<uint32_t> rids;
    rids.reserve(ids.size()); // 预分配内存
    // 此处不用实现（保留原有逻辑）
    return rids;
}

/**
 * @brief 模拟邻接查询函数（实际需替换为业务层的真实查询逻辑）
 * @param id 节点ID
 * @return 邻接关系列表
 */
inline std::vector<Adjacent> query_adjacent(uint32_t id)
{
    // 模拟数据：每个ID返回2个邻接关系（仅补全模拟数据，不改动逻辑）
    std::vector<Adjacent> elems;
    elems.push_back({Rel{static_cast<int32_t>(id + 100), "rel_" + std::to_string(id)}, id + 1000});
    elems.push_back({Rel{static_cast<int32_t>(id + 200), "rel_" + std::to_string(id)}, id + 2000});
    return elems;
}

// 字符串列表去重工具方法
inline std::vector<std::string> deduplicate(const std::vector<std::string> &input)
{
    std::unordered_set<std::string> dedup_set;
    std::vector<std::string> result;
    result.reserve(input.size()); // 预分配内存，提升效率

    for (const auto &str : input)
    {
        // 跳过空字符串（可选，根据业务需求调整）
        if (str.empty())
        {
            continue;
        }
        // 未出现过的字符串加入结果
        if (dedup_set.insert(str).second)
        {
            result.push_back(str);
        }
    }

    return result;
}

// 字符串列表去重工具方法2
inline std::unordered_set<std::string> dedup(const std::vector<std::string>& input) {
    std::unordered_set<std::string> dedup_set;
    for (const auto& label : input) {
        if (!label.empty()) { // 跳过空标签
            dedup_set.insert(label);
        }
    }
    return dedup_set;
}