#include <iostream>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include "../include/LabelRegistry.hpp"
#include "xxh3.h"

// ------------------------------------------------------------
// Helper: fast line parsing (no copy)
struct ParsedEdge {
    std::string_view src_id;
    std::string_view src_label;
    std::string_view dst_id;
    std::string_view dst_label;
    std::string_view edge_label;
};

ParsedEdge parse_csv_line(std::string_view line) {
    size_t p0 = line.find(',');
    if (p0 == std::string_view::npos) throw std::runtime_error("Invalid line");
    size_t p1 = line.find(',', p0 + 1);
    if (p1 == std::string_view::npos) throw std::runtime_error("Invalid line");
    size_t p2 = line.find(',', p1 + 1);
    if (p2 == std::string_view::npos) throw std::runtime_error("Invalid line");
    size_t p3 = line.find(',', p2 + 1);
    if (p3 == std::string_view::npos) throw std::runtime_error("Invalid line");

    return {
        line.substr(0, p0),
        line.substr(p0 + 1, p1 - p0 - 1),
        line.substr(p1 + 1, p2 - p1 - 1),
        line.substr(p2 + 1, p3 - p2 - 1),
        line.substr(p3 + 1)
    };
}

// ------------------------------------------------------------
// Helper: write binary vector
template <typename T>
void write_binary_file(const std::string& path, const std::vector<T>& data) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
}

// ------------------------------------------------------------
// Helper: transpose CSR
void transpose_csr(
    const std::vector<uint32_t>& out_offsets,
    const std::vector<uint32_t>& out_neighbors,
    const std::vector<uint8_t>& out_edge_labels,
    size_t num_nodes,
    std::vector<uint32_t>& in_offsets,
    std::vector<uint32_t>& in_neighbors,
    std::vector<uint8_t>& in_edge_labels
) {
    // Step 1: count in-degrees
    std::vector<uint32_t> in_degree(num_nodes, 0);
    for (uint32_t v : out_neighbors) {
        in_degree[v]++;
    }

    // Step 2: compute in_offsets
    in_offsets.resize(num_nodes + 1);
    in_offsets[0] = 0;
    for (size_t i = 0; i < num_nodes; i++) {
        in_offsets[i + 1] = in_offsets[i] + in_degree[i];
    }

    // Step 3: fill in_neighbors and in_edge_labels
    in_neighbors.resize(out_neighbors.size());
    in_edge_labels.resize(out_edge_labels.size());
    
    // Reset in_degree for reuse as position counters
    std::fill(in_degree.begin(), in_degree.end(), 0);

    for (uint32_t u = 0; u < num_nodes; u++) {
        uint32_t start = out_offsets[u];
        uint32_t end = out_offsets[u + 1];
        for (uint32_t i = start; i < end; i++) {
            uint32_t v = out_neighbors[i];
            uint32_t pos = in_offsets[v] + in_degree[v]++;
            in_neighbors[pos] = u;
            in_edge_labels[pos] = out_edge_labels[i];
        }
    }
}

// ------------------------------------------------------------
// Main import function
int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_csv> <output_dir>" << std::endl;
        return 1;
    }

    std::string input_csv = argv[1];
    std::string output_dir = argv[2];

    // Create output directory
    std::filesystem::create_directories(output_dir);

    try {
        // ------------------------
        // Pass 1: Collect all external IDs and assign internal IDs
        // ------------------------
        std::cout << "Pass 1: Collecting node IDs..." << std::endl;

        std::unordered_map<std::string, uint32_t> external_to_internal;
        std::unordered_map<std::string, uint8_t> node_label_map;
        LabelRegistry node_label_registry;
        LabelRegistry edge_label_registry;

        {
            std::ifstream infile(input_csv);
            if (!infile) {
                throw std::runtime_error("Failed to open input file: " + input_csv);
            }

            std::string line;
            std::getline(infile, line); // Skip header

            while (std::getline(infile, line)) {
                try {
                    ParsedEdge edge = parse_csv_line(line);

                    // Register source node
                    if (external_to_internal.find(std::string(edge.src_id)) == external_to_internal.end()) {
                        uint32_t internal_id = external_to_internal.size();
                        external_to_internal[std::string(edge.src_id)] = internal_id;
                    }

                    // Register destination node
                    if (external_to_internal.find(std::string(edge.dst_id)) == external_to_internal.end()) {
                        uint32_t internal_id = external_to_internal.size();
                        external_to_internal[std::string(edge.dst_id)] = internal_id;
                    }

                    // Register labels
                    node_label_registry.get_or_assign(std::string(edge.src_label));
                    node_label_registry.get_or_assign(std::string(edge.dst_label));
                    edge_label_registry.get_or_assign(std::string(edge.edge_label));

                } catch (const std::exception& e) {
                    std::cerr << "Warning: " << e.what() << " (skipping line)" << std::endl;
                    continue;
                }
            }
        }

        size_t num_nodes = external_to_internal.size();
        std::cout << "Found " << num_nodes << " nodes" << std::endl;

        // ------------------------
        // Prepare ID mapping (sorted by hash for binary search)
        // ------------------------
        std::vector<std::pair<uint64_t, uint32_t>> hash_to_internal;
        std::vector<uint8_t> node_labels(num_nodes);

        for (const auto& [external_id, internal_id] : external_to_internal) {
            uint64_t hash = XXH3(external_id.data(), external_id.size());
            hash_to_internal.emplace_back(hash, internal_id);
        }

        // Sort by hash
        std::sort(hash_to_internal.begin(), hash_to_internal.end());

        // Extract sorted hashes and internal IDs
        std::vector<uint64_t> hashes(num_nodes);
        std::vector<uint32_t> internal_ids(num_nodes);
        for (size_t i = 0; i < num_nodes; i++) {
            hashes[i] = hash_to_internal[i].first;
            internal_ids[i] = hash_to_internal[i].second;
        }

        // ------------------------
        // Pass 2: Build CSR
        // ------------------------
        std::cout << "Pass 2: Building CSR..." << std::endl;

        std::vector<uint32_t> out_offsets(num_nodes + 1, 0);
        std::vector<std::vector<std::pair<uint32_t, uint8_t>>> adj_list(num_nodes);

        {
            std::ifstream infile(input_csv);
            std::string line;
            std::getline(infile, line); // Skip header

            while (std::getline(infile, line)) {
                try {
                    ParsedEdge edge = parse_csv_line(line);

                    uint32_t src_internal = external_to_internal.at(std::string(edge.src_id));
                    uint32_t dst_internal = external_to_internal.at(std::string(edge.dst_id));
                    uint8_t edge_label_id = edge_label_registry.get_or_assign(std::string(edge.edge_label));

                    adj_list[src_internal].emplace_back(dst_internal, edge_label_id);
                    out_offsets[src_internal + 1]++;

                } catch (const std::exception& e) {
                    continue;
                }
            }
        }

        // Compute prefix sums for offsets
        for (size_t i = 1; i <= num_nodes; i++) {
            out_offsets[i] += out_offsets[i - 1];
        }

        size_t num_edges = out_offsets[num_nodes];
        std::cout << "Found " << num_edges << " edges" << std::endl;

        // Fill neighbors and edge labels
        std::vector<uint32_t> out_neighbors(num_edges);
        std::vector<uint8_t> out_edge_labels(num_edges);

        for (uint32_t u = 0; u < num_nodes; u++) {
            uint32_t start = out_offsets[u];
            for (size_t i = 0; i < adj_list[u].size(); i++) {
                out_neighbors[start + i] = adj_list[u][i].first;
                out_edge_labels[start + i] = adj_list[u][i].second;
            }
        }

        // ------------------------
        // Build transposed CSR (in_edges)
        // ------------------------
        std::cout << "Building transposed CSR..." << std::endl;

        std::vector<uint32_t> in_offsets;
        std::vector<uint32_t> in_neighbors;
        std::vector<uint8_t> in_edge_labels;

        transpose_csr(out_offsets, out_neighbors, out_edge_labels, num_nodes,
                     in_offsets, in_neighbors, in_edge_labels);

        // ------------------------
        // Write output files
        // ------------------------
        std::cout << "Writing output files..." << std::endl;

        // Meta data
        {
            std::ofstream meta_file(output_dir + "/meta.bin", std::ios::binary);
            meta_file.write(reinterpret_cast<const char*>(&num_nodes), sizeof(num_nodes));
            meta_file.write(reinterpret_cast<const char*>(&num_edges), sizeof(num_edges));
        }

        // ID mapping
        write_binary_file(output_dir + "/id_hashes.bin", hashes);
        write_binary_file(output_dir + "/id_internal_ids.bin", internal_ids);

        // CSR data
        write_binary_file(output_dir + "/out_offsets.bin", out_offsets);
        write_binary_file(output_dir + "/out_neighbors.bin", out_neighbors);
        write_binary_file(output_dir + "/out_edge_labels.bin", out_edge_labels);

        write_binary_file(output_dir + "/in_offsets.bin", in_offsets);
        write_binary_file(output_dir + "/in_neighbors.bin", in_neighbors);
        write_binary_file(output_dir + "/in_edge_labels.bin", in_edge_labels);

        // Label strings
        {
            std::ofstream node_labels_file(output_dir + "/node_label_strings.txt");
            for (const auto& label : node_label_registry.get_strings()) {
                node_labels_file << label << std::endl;
            }

            std::ofstream edge_labels_file(output_dir + "/edge_label_strings.txt");
            for (const auto& label : edge_label_registry.get_strings()) {
                edge_labels_file << label << std::endl;
            }
        }

        std::cout << "Import completed successfully!" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}