#pragma once
#include <string>
#include <vector>
#include <unordered_map>

class LabelRegistry {
    std::unordered_map<std::string, uint8_t> str_to_id_;
    std::vector<std::string> id_to_str_;

public:
    uint8_t get_or_assign(const std::string& s) {
        auto it = str_to_id_.find(s);
        if (it != str_to_id_.end()) return it->second;
        uint8_t id = static_cast<uint8_t>(id_to_str_.size());
        if (id >= 255) throw std::runtime_error("Too many labels (max 255)");
        str_to_id_[s] = id;
        id_to_str_.push_back(s);
        return id;
    }

    const std::vector<std::string>& get_strings() const { return id_to_str_; }
};