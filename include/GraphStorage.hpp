#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include "MmappedCSR.hpp"
#include "IdMapper.hpp"

class GraphStorage {
public:
    void load(const std::string& dir);
    
    const MmappedCSR& get_out_csr() const { return out_csr_; }
    const MmappedCSR& get_in_csr() const { return in_csr_; }
    const IdMapper& get_id_mapper() const { return id_mapper_; }
    
    size_t get_num_nodes() const { return num_nodes_; }
    size_t get_num_edges() const { return num_edges_; }
    
    const uint8_t* get_node_labels() const { return node_labels_; }
    const std::vector<std::string>& get_node_label_strings() const { return node_label_strings_; }
    const std::vector<std::string>& get_edge_label_strings() const { return edge_label_strings_; }

private:
    MmappedCSR out_csr_;
    MmappedCSR in_csr_;
    IdMapper id_mapper_;
    size_t num_nodes_ = 0;
    size_t num_edges_ = 0;
    uint8_t* node_labels_ = nullptr;
    size_t node_labels_mapped_size_ = 0;
    std::vector<std::string> node_label_strings_;
    std::vector<std::string> edge_label_strings_;
};