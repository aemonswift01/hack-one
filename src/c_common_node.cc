#include "c_common_node.h"
#include <stdexcept>
#include <iostream>
#include <algorithm>
#include "entity-cc.h"

CommonNode::CommonNode(const std::string &left_id,
                       const std::string &right_id,
                       const std::vector<std::string> &labels)
{
    this->left_id = left_id;
    this->right_id = right_id;
    this->labelSet = dedup(labels);
}

// 核心查询方法：计算左右节点的公共邻接节点数
size_t CommonNode::query()
{
    // 校验ID有效性
    if (left_id.empty() || right_id.empty())
    {
        return 0;
    }

    std::vector<uint32_t> ids = batch_convert_node_id({left_id, right_id});

    roaring::Roaring left_adj = collect(ids[0]);
    roaring::Roaring right_adj = collect(ids[1]);

    return left_adj.and_cardinality(right_adj);
}

roaring::Roaring CommonNode::collect(size_t id)
{
    roaring::Roaring bitmap;
    std::vector<Adjacent> adj_results = query_adjacent(id);
    for (const auto &adj : adj_results)
    {
        // 仅收集labelSet中存在的标签对应的邻接节点
        // todo 此处需要考虑标签
        if (contain(adj.rel.name))
        {
            bitmap.add(adj.otherId);
        }
    }
    bitmap.runOptimize();
    bitmap.shrinkToFit();

    return bitmap;
}

bool CommonNode::contain(const std::string &label)
{
    if (labelSet.empty())
    {
        return true;
    }
    return labelSet.count(label) != 0;
}