#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <stdexcept>
#include "c_k_hop.h"

// todo-1  ids的去重问题,假如有重复的，返回不应该重复返回。应该做映射处理
// todo-2  dfs的实现问题，注意初始化size的问题，参考已实现的代码
// todo-3  dfs实现需要考虑迭代器
// 条件标签是过滤边标签还是点标签？？？
// todo-4  处理边界检查的问题 后续处理
//  还未调试
// roaring的压缩问题??

// 补充构造方法（仅初始化成员，不改动原有逻辑）
KHop::KHop(std::vector<std::string> &ids_ref,
           std::vector<std::string> &labels_ref,
           uint32_t size_val)
    : ids(ids_ref), labels(labels_ref), size(size_val), mem_limit((size_t)1600 * 1024 * 1024)
{
    // 初始化counts为与ids等长的0值（仅补全初始化，不改动逻辑）
    counts.resize(ids.size(), 0);
    visited.resize(size_val);
}

/**
 *
 * @brief 处理单个节点ID的邻接关系
 * @param id 待处理的uint32_t类型节点ID
 * @return 处理后的有效关系数量（示例返回0，可按需修改）
 */
uint64_t KHop::k_kop_b(uint32_t id, uint64_t count)
{
    // 记录
    count++;
    Link_Node linkNode;
    linkNode.update_node(id);
    std::vector<Link_Node> arrLinkNode;
    arrLinkNode.reserve(1); // 优化：预分配初始内存（已知初始只有1个节点）
    arrLinkNode.push_back(linkNode);

    for (size_t i = 0; i < size; i++)
    {
        // 此处不能扩容，引起内存溢出
        std::vector<Link_Node> tmpArrLinkNode;
        if (check_limit0(arrLinkNode, tmpArrLinkNode, size) && i > 0)
        {
            // todo 进入dfs模式
        }
        // 此处 需要提前计算内存，避免OOM
        tmpArrLinkNode.reserve(arrLinkNode.size() * size);

        // 优化1：改为const引用遍历，避免Link_Node的浅拷贝开销
        for (const Link_Node &node : arrLinkNode)
        {
            // 通过id查询邻接关系（原有逻辑不变）
            const std::vector<Adjacent> &adjIter = query_adjacent(node.endid); // 优化：const引用避免拷贝

            // 遍历邻接关系（原有逻辑不变）
            for (const auto &elem : adjIter)
            {
                // 尝试添加rel.id，存在则跳过
                if (node.exist(elem.rel.rid))
                {
                    continue;
                }
                Link_Node newNode = node.clone();
                newNode.update_node(elem.otherId);
                newNode.add_rel(elem.rel.rid);
                count++;
                update_offset_mem(newNode);

                // 内存检查（原有逻辑不变）
                if (check_limit1(arrLinkNode, tmpArrLinkNode))
                {
                    // todo 进入dfs模式
                }

                // 添加到临时数组（原有逻辑不变）
                tmpArrLinkNode.push_back(std::move(newNode)); // 优化：move减少拷贝
            }
        }

        // 赋值给arrLinkNode（原有逻辑不变）
        arrLinkNode = std::move(tmpArrLinkNode); // 优化：move转移资源，避免深拷贝

        // 优化：清空临时数组（辅助内存回收，无逻辑影响）
        tmpArrLinkNode.clear();
        update_vec_mem_cache(arrLinkNode);

        // 此处的node需要回收，暂不实现（保留原有注释）
    }
}

// todo
void KHop::k_kop_d(uint32_t id, int64_t count)
{
}

/**
 * @brief 计算节点数量（含string ID转uint32_t+邻接关系处理）
 * @return 有效节点处理后的总关系数量
 */
void KHop::query()
{
    // 空ID列表直接返回0
    if (ids.empty())
    {
        return;
    }

    // 批量转换string ID为uint32_t
    std::vector<uint32_t> rids = batch_convert_node_id(ids);

    // 统计所有节点处理后的有效关系总数
    int total_count = 0;
    for (int i = 0; i < static_cast<int>(rids.size()); i++)
    {
        uint32_t id = rids[i];
        counts[i] = k_kop_b(id, counts[i]);
    }
}

bool KHop::check_limit0(std::vector<Link_Node> arrLinkNode0, std::vector<Link_Node> arrLinkNode1, int size)
{
    offset_mem = sizeof(arrLinkNode1) + sizeof(Link_Node) * size;
    return is_over_limit(cached_total_mem + offset_mem, mem_limit);
}

bool KHop::check_limit1(std::vector<Link_Node> arrLinkNode0, std::vector<Link_Node> arrLinkNode1)
{
    return is_over_limit(cached_total_mem + offset_mem, mem_limit);
}

void KHop::update_vec_mem_cache(const std::vector<Link_Node> &vec)
{
    offset_mem = 0;
    cached_total_mem = sizeof(vec); // vector自身开销
    // const迭代器遍历：减少边界检查，提升效率
    for (auto it = vec.cbegin(); it != vec.cend(); ++it)
    {
        cached_total_mem += it->mem();
    }
}

void KHop::update_offset_mem(Link_Node node)
{
    offset_mem = offset_mem + node.mem() - sizeof(node);
}
