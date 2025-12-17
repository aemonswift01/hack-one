#include <stdexcept>
#include <iostream>
#include <unordered_set>
#include <algorithm>
#include <vector>
#include <numeric> // for iota
#include <unordered_map>
#include "c_conn_comp.h"
#include "entity-cc.h"


// todo 标签未生效，需确定需求再进行处理

// 构造函数实现：去重+初始化
// 1. 对输入IDs去重并赋值给unique_ids，空则抛异常
// 2. 对输入Labels去重并赋值给unique_labels，空则抛异常
// 3. 初始化arrAdj：长度与去重后Labels一致，每个元素为空Roaring位图
ConnComp::ConnComp(const std::vector<std::string> &ids, const std::vector<std::string> &labels)
{
    // 1. 对IDs去重并初始化
    unique_ids = deduplicate(ids);
    if (unique_ids.empty())
    {
        throw std::invalid_argument("IDs list is empty after deduplication");
    }

    // 2. 对Labels去重并初始化
    unique_labels = deduplicate(labels);
    if (unique_labels.empty())
    {
        throw std::invalid_argument("Labels list is empty after deduplication");
    }

    // 3. 初始化arrAdj：长度等于去重后labels的长度，每个元素为空Roaring位图
    arrAdj.resize(unique_labels.size());
}

size_t ConnComp::query()
{
    std::vector<uint32_t> ids = batch_convert_node_id(unique_ids);
    const size_t ids_size = ids.size();
    // 下标遍历：直接通过i获取ids的索引
    for (size_t idx = 0; idx < ids_size; ++idx)
    {
        uint32_t id = ids[idx];
        // 此处需要获取adj的索引i，从而获取对应roaringAdj=arrAdj[i]
        roaring::Roaring &roaringAdj = arrAdj[idx];
        // 将当前ID加入对应Roaring位图
        roaringAdj.add(id);
        //  调用外部邻接查询函数
        std::vector<Adjacent> adjs = query_adjacent(id);

        // 遍历邻接结果，将邻接节点ID加入对应位图
        for (const auto &adj : adjs)
        {
            roaringAdj.add(adj.otherId);
        }
    }
    // 返回处理的邻接节点总数（需补充统计逻辑）
    std::vector<roaring::Roaring> merge = intersect_merge(arrAdj);
    return merge.size();
}

// ========== 终极时间优化的合并函数 ==========
std::vector<roaring::Roaring> ConnComp::intersect_merge(std::vector<roaring::Roaring> arrAdj)
{
    // ========== 优化1：极致预处理（过滤+预优化，减少后续计算） ==========
    // 1.1 过滤空位图（裸循环比remove_if快，减少STL函数开销）
    size_t valid_size = 0;
    for (size_t i = 0; i < arrAdj.size(); ++i)
    {
        if (!arrAdj[i].isEmpty())
        {
            if (i != valid_size)
            {
                // 移动而非拷贝（减少内存操作）
                arrAdj[valid_size] = std::move(arrAdj[i]);
            }
            valid_size++;
        }
    }
    arrAdj.resize(valid_size);
    if (arrAdj.empty())
    {
        std::cerr << "intersect_merge: empty after filter" << std::endl;
        return {};
    }
    if (arrAdj.size() == 1)
    {
        return arrAdj;
    }
    const size_t n = arrAdj.size();

    // 1.2 Roaring极致优化（批量压缩，减少后续位运算耗时）
    // runOptimize：压缩为Run模式，位图越大，提速越明显
    // shrinkToFit：释放冗余内存，减少缓存失效
    for (auto &r : arrAdj)
    {
        r.runOptimize();
        r.shrinkToFit();
    }

    // ========== 优化2：预缓存根节点+减少内存访问 ==========
    FastDSU dsu(n);
    // 预缓存每个位图的指针（减少vector[]的间接访问）
    const roaring::Roaring *arr_ptr = arrAdj.data();

    // ========== 优化3：循环展开+减少分支（核心时间优化） ==========
    // 遍历i<j，且循环展开4次（减少循环判断开销）
    for (size_t i = 0; i < n; ++i)
    {
        const auto &ri = arr_ptr[i];
        const size_t ri_root = dsu.find(i);

        // 循环展开4次（针对CPU流水线优化）
        size_t j = i + 1;
        for (; j + 3 < n; j += 4)
        {
            // 批量判断+合并，减少分支预测失败
            if (dsu.find(j) != ri_root && has_intersection(ri, arr_ptr[j]))
                dsu.unite(i, j);
            if (dsu.find(j + 1) != ri_root && has_intersection(ri, arr_ptr[j + 1]))
                dsu.unite(i, j + 1);
            if (dsu.find(j + 2) != ri_root && has_intersection(ri, arr_ptr[j + 2]))
                dsu.unite(i, j + 2);
            if (dsu.find(j + 3) != ri_root && has_intersection(ri, arr_ptr[j + 3]))
                dsu.unite(i, j + 3);
        }
        // 处理剩余元素
        for (; j < n; ++j)
        {
            if (dsu.find(j) != ri_root && has_intersection(ri, arr_ptr[j]))
            {
                dsu.unite(i, j);
            }
        }
    }

    // ========== 优化4：批量合并（减少Roaring操作次数） ==========
    // 用vector替代unordered_map（连续内存，缓存友好）
    std::vector<roaring::Roaring> component_vec(n);
    std::vector<bool> is_root(n, false); // 标记是否为根节点

    // 第一步：标记根节点+初始化分量位图
    for (size_t i = 0; i < n; ++i)
    {
        size_t root = dsu.find(i);
        if (!is_root[root])
        {
            is_root[root] = true;
            component_vec[root] = std::move(arrAdj[i]); // 移动，无拷贝
        }
    }

    // 第二步：批量合并（减少runOptimize调用次数）
    for (size_t i = 0; i < n; ++i)
    {
        size_t root = dsu.find(i);
        if (!is_root[root])
        { // 非根节点，合并到根
            component_vec[root] |= arrAdj[i];
        }
    }

    // 第三步：统一优化+过滤（减少多次优化的开销）
    std::vector<roaring::Roaring> result;
    result.reserve(n);
    for (size_t i = 0; i < n; ++i)
    {
        if (is_root[i] && !component_vec[i].isEmpty())
        {
            component_vec[i].runOptimize();
            component_vec[i].shrinkToFit();
            result.push_back(std::move(component_vec[i]));
        }
    }

    // ========== 调试输出（可选，生产环境注释） ==========
    std::cout << "intersect_merge (ultra-optimized) completed: " << result.size() << " groups" << std::endl;
    return result;
}

// 备用
// std::vector<roaring::Roaring> ConnComp::intersect_merge(std::vector<roaring::Roaring> arrAdj)
// {
//     // ========== 预处理（基础优化，必做） ==========
//     // 1. 过滤空位图
//     arrAdj.erase(
//         std::remove_if(arrAdj.begin(), arrAdj.end(),
//                        [](const roaring::Roaring &r)
//                        { return r.isEmpty(); }),
//         arrAdj.end());
//     if (arrAdj.empty())
//     {
//         std::cerr << "intersect_merge: arrAdj is empty after filter, return empty" << std::endl;
//         return {};
//     }
//     size_t n = arrAdj.size();
//     if (n == 1)
//     {
//         return arrAdj;
//     }

//     // 2. 位图预优化（加速后续交集计算）
//     for (auto &r : arrAdj)
//     {
//         r.runOptimize(); // 压缩存储，提升交集/并集效率
//     }

//     // ========== 核心1：DSU 标记连通分量 ==========
//     DSU dsu(n);
//     // 遍历所有位图对（i < j，避免重复比对）
//     for (size_t i = 0; i < n; ++i)
//     {
//         const auto &ri = arrAdj[i];
//         // 优化：提前获取i的根节点，减少后续find调用
//         size_t ri_root = dsu.find(i);

//         for (size_t j = i + 1; j < n; ++j)
//         {
//             // 若j已和i同属一个分量，跳过
//             if (dsu.find(j) == ri_root)
//                 continue;

//             // 快速判断交集：Roaring 底层位运算优化，极快
//             roaring::Roaring intersection = ri;
//             intersection &= arrAdj[j];
//             if (!intersection.isEmpty())
//             {
//                 dsu.unite(i, j); // 交集非空，合并连通分量
//             }
//         }
//     }

//     // ========== 核心2：按连通分量批量合并 ==========
//     // 键：连通分量根节点，值：该分量的合并位图
//     std::unordered_map<size_t, roaring::Roaring> component_map;
//     for (size_t i = 0; i < n; ++i)
//     {
//         size_t root = dsu.find(i); // 找i的根节点
//         if (component_map.count(root) == 0)
//         {
//             component_map[root] = arrAdj[i]; // 初始化分量位图
//         }
//         else
//         {
//             component_map[root] |= arrAdj[i];  // 合并为并集
//             component_map[root].runOptimize(); // 实时优化，减少内存
//         }
//     }

//     // ========== 结果整理 ==========
//     std::vector<roaring::Roaring> result;
//     result.reserve(component_map.size()); // 预分配内存
//     for (auto &[root, bitmap] : component_map)
//     {
//         result.push_back(std::move(bitmap)); // 移动语义，避免拷贝
//     }

//     return result;
// }