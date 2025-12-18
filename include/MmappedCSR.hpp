#pragma once
#include <cstddef>
#include <cstdint>

class MmappedCSR {
    const uint32_t* offsets_ = nullptr;
    const uint32_t* neighbors_ = nullptr;
    const uint8_t* edge_labels_ = nullptr;
    size_t num_nodes_ = 0;
    size_t mapped_size_offsets_ = 0;
    size_t mapped_size_neighbors_ = 0;
    size_t mapped_size_labels_ = 0;

public:
    ~MmappedCSR();
    void load(const std::string& prefix);

    struct EdgeView {
        const uint32_t* neighbors;
        const uint8_t* edge_labels;
        size_t count;
    };

    EdgeView out_edges(uint32_t node_id) const {
        uint32_t start = offsets_[node_id];
        uint32_t end = offsets_[node_id + 1];
        return { neighbors_ + start, edge_labels_ + start, end - start };
    }
};