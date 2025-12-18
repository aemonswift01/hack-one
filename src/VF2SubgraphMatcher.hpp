#pragma once
#include <vector>
#include <cstdint>
#include "GraphStorage.hpp"

class VF2SubgraphMatcher {
public:
    VF2SubgraphMatcher(const GraphStorage& graph);
    
    uint64_t count_matches(const std::vector<uint8_t>& pattern_node_labels,
                          const std::vector<std::pair<uint32_t, uint32_t>>& pattern_edges,
                          const std::vector<uint8_t>& pattern_edge_labels);

private:
    const GraphStorage& graph_;
    
    struct State {
        std::vector<uint32_t> core_1;    // pattern node -> graph node
        std::vector<uint32_t> core_2;    // graph node -> pattern node
        std::vector<bool> in_1;          // pattern node in mapping
        std::vector<bool> in_2;          // graph node in mapping
        std::vector<uint32_t> candidate_pairs;  // (p, g) pairs
    };
    
    bool is_isomorphic(const State& state, const std::vector<uint8_t>& pattern_node_labels,
                      const std::vector<std::pair<uint32_t, uint32_t>>& pattern_edges,
                      const std::vector<uint8_t>& pattern_edge_labels);
    
    uint64_t backtrack(State& state, const std::vector<uint8_t>& pattern_node_labels,
                      const std::vector<std::pair<uint32_t, uint32_t>>& pattern_edges,
                      const std::vector<uint8_t>& pattern_edge_labels);
};