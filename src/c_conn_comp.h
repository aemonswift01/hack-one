#ifndef CONN_COMP_H
#define CONN_COMP_H

#include <vector>
#include <string>
#include <unordered_set>
#include "roaring.hh"


// 连通分量结构体
struct ConnComp {
    // 成员变量
    std::vector<std::string> unique_ids;    // 去重后的字符串ID列表
    std::vector<std::string> unique_labels; // 去重后的字符串标签列表
    std::vector<roaring::Roaring> arrAdj;   // Roaring位图列表，与labels长度一致

    // 构造函数
    // @param ids: 原始ID列表（需去重）
    // @param labels: 原始标签列表（需去重）
    // @note: 会先对ids和labels去重，再初始化arrAdj（长度等于去重后labels的长度）
    ConnComp(const std::vector<std::string>& ids, const std::vector<std::string>& labels);

    // 核心查询方法
    // @param query_func: 邻接查询函数（外部实现，输入ID返回邻接结果）
    // @note: 遍历unique_ids，调用query_adjacent，处理邻接结果
    size_t query();
    
    std::vector<roaring::Roaring> intersect_merge(std::vector<roaring::Roaring> arrAdj);
};

#endif // CONN_COMP_;