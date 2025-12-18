#pragma once
#include <string>
#include <string_view>
#include <optional>
#include <cstdint>

class IdMapper {
    const uint64_t* hashes_ = nullptr;
    const uint32_t* ids_ = nullptr;
    size_t num_nodes_ = 0;
    size_t mapped_size_ = 0;

public:
    ~IdMapper();
    void load(const std::string& dir);
    std::optional<uint32_t> external_id_to_internal(std::string_view external_id) const;

private:
    static uint64_t hash_string(std::string_view s);
};