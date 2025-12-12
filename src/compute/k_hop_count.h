// #include "deps/CRoaring/include/roaring/roaring.h"

#include <stdint.h>
#include <iostream>
#include <vector>

class k_hop_count {
   private:
   private:
    std::vector<std::string> items_;  // string 类型的列表（主数据）
    int length_;  // int 类型的长度（可选：你也可以用 size_t）
    std::vector<std::string> labels_;  // string 类型的 label 列表
                                       //初始化接口
   public:
    uint64_t kHopCount() const {
        std::cout << "uuuuu";
        return 0;
    };

    // 构造函数（可选：提供默认构造、带参构造等）
    k_hop_count() = default;

    k_hop_count(const std::vector<std::string>& items, int length,
                const std::vector<std::string>& labels)
        : items_(items), length_(length), labels_(labels) {}

    // 如果希望支持移动语义（高效）
    k_hop_count(std::vector<std::string>&& items, int length,
                std::vector<std::string>&& labels)
        : items_(std::move(items)),
          length_(length),
          labels_(std::move(labels)) {}

    // Getter（推荐 const 修饰）
    const std::vector<std::string>& getItems() const { return items_; }

    int getLength() const { return length_; }

    const std::vector<std::string>& getLabels() const { return labels_; }

    // Setter（按需添加）
    void setItems(const std::vector<std::string>& items) { items_ = items; }

    void setLength(int length) { length_ = length; }

    void setLabels(const std::vector<std::string>& labels) { labels_ = labels; }
};
