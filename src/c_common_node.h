#ifndef COMMON_NODE_H
#define COMMON_NODE_H

#include <vector>
#include <string>
#include <unordered_set>
#include "roaring.hh"


// 公共节点核心结构体
struct CommonNode {
    // 成员变量
    std::string left_id;                
    std::string right_id;
    std::unordered_set<std::string> labelSet;

    // 构造函数
    // @param left_id: 左侧节点ID
    // @param right_id: 右侧节点ID
    // @param labels: 原始标签列表（自动去重后存入labelSet）
    // @throw std::invalid_argument: 空ID或空标签列表（去重后）抛出异常
    CommonNode(const std::string& left_id, 
               const std::string& right_id, 
               const std::vector<std::string>& labels);

    // 核心查询方法：基于左右节点ID+标签集查询公共节点
    // @return: 公共节点的总数
    // @throw std::runtime_error: 查询函数未初始化/ID无效时抛出异常
    size_t query();

    // 收集指定节点ID的邻接节点到Roaring位图
    // @param node_id: 待收集的节点ID
    // @return: 包含该节点所有邻接节点ID的Roaring位图
    // @throw std::invalid_argument: 空节点ID抛出异常
    roaring::Roaring collect(size_t id);

    bool contain(const std::string& label);
};

#endif