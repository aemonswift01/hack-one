#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include "roaring-c.h"
#include "c_k_hop.h"
#include "c_conn_comp.h"
#include "c_common_node.h"

// 测试主函数
int main()
{
    std::vector<std::string> test_ids = {"1001", "1002", "1003"};
    std::vector<std::string> test_labels = {"user", "admin", "guest"};

    // 修正：query是k_hop的成员函数，需先创建对象再调用
    KHop kh(test_ids, test_labels, 2); // 创建k_hop对象，size=2，limit默认1024M
    kh.query();

    for (const auto &num : kh.counts)
    {
        std::cout << num << ",";
    }
    std::cout << "k_hop_count 完成执行" << std::endl;

    ConnComp conn(test_ids, test_labels);
    size_t count = conn.query();

    for (const auto &num : kh.counts)
    {
        std::cout << num << ",";
    }

    std::cout << "conn_comp 完成执行:" << count << std::endl;

    CommonNode common(test_ids[0],test_ids[0], test_labels);
    size_t count0 = common.query();

    std::cout << "common_node 完成执行:" << count0 << std::endl;
    return 0;
}