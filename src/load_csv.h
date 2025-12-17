#pragma once
#include "s_storage.h"
#include "s_cache.h"
#include <fstream>
#include <thread>
#include <atomic>

struct LabelMapping {
    absl::flat_hash_map<std::string, uint8_t> str_to_id;
    absl::flat_hash_map<uint8_t, std::string> id_to_str;
};

struct StringIdMapping {
    absl::flat_hash_map<std::string, uint32_t> str_to_id;
    absl::flat_hash_map<uint32_t, std::string> id_to_str;
    std::atomic<uint32_t> next_id = 0;

    uint32_t GetIntId(const std::string& str) {
        auto it = str_to_id.find(str);
        if (it != str_to_id.end()) return it->second;
        uint32_t id = next_id.fetch_add(1);
        str_to_id[str] = id;
        id_to_str[id] = str;
        return id;
    }
};

class CSVLoader {
private:
    std::atomic<uint32_t> next_edge_id = 0;
    std::atomic<uint8_t> next_edge_label_id = 1;
    std::atomic<uint8_t> next_point_label_id = 1;

    uint8_t GetEdgeLabelId(const std::string& label, LabelMapping& mapping) {
        auto it = mapping.str_to_id.find(label);
        if (it != mapping.str_to_id.end()) return it->second;
        uint8_t id = next_edge_label_id.fetch_add(1);
        mapping.str_to_id[label] = id;
        mapping.id_to_str[id] = label;
        return id;
    }

    uint8_t GetPointLabelId(const std::string& label, LabelMapping& mapping) {
        auto it = mapping.str_to_id.find(label);
        if (it != mapping.str_to_id.end()) return it->second;
        uint8_t id = next_point_label_id.fetch_add(1);
        mapping.str_to_id[label] = id;
        mapping.id_to_str[id] = label;
        return id;
    }

    void ParseCSVRecord(const std::string& line, StringIdMapping& point_id_map,
                        LabelMapping& point_label_map, LabelMapping& edge_label_map,
                        std::unordered_map<uint32_t, Block>& block_builders) {
        std::vector<std::string> fields;
        size_t pos = 0;
        while ((pos = line.find(',')) != std::string::npos) {
            fields.push_back(line.substr(0, pos));
            line.substr(pos + 1);
        }
        fields.push_back(line);

        std::string start_str = fields[0];
        std::string end_str = fields[1];
        std::string edge_label = fields[2];
        std::string start_label = fields[3];
        std::string end_label = fields[4];

        uint32_t start_id = point_id_map.GetIntId(start_str);
        uint32_t end_id = point_id_map.GetIntId(end_str);
        uint8_t el_id = GetEdgeLabelId(edge_label, edge_label_map);
        uint8_t sl_id = GetPointLabelId(start_label, point_label_map);
        uint8_t el_id2 = GetPointLabelId(end_label, point_label_map);

        uint32_t edge_id = next_edge_id.fetch_add(1);
        uint32_t start_block_id = start_id / BLOCK_SIZE;
        uint32_t end_block_id = end_id / BLOCK_SIZE;
        uint32_t start_local_id = start_id % BLOCK_SIZE;
        uint32_t end_local_id = end_id % BLOCK_SIZE;

        // 构建出边
        EdgeData out_edge;
        out_edge.edge_id = edge_id;
        out_edge.node_id = end_id;
        out_edge.edge_label_id = el_id;
        out_edge.is_out = 1;
        out_edge.start_label_id = sl_id;
        out_edge.end_label_id = el_id2;

        // 构建入边
        EdgeData in_edge;
        in_edge.edge_id = edge_id;
        in_edge.node_id = start_id;
        in_edge.edge_label_id = el_id;
        in_edge.is_out = 0;
        in_edge.start_label_id = el_id2;
        in_edge.end_label_id = sl_id;

        // 写入出边块
        if (!block_builders.count(start_block_id)) {
            block_builders[start_block_id] = Block();
            block_builders[start_block_id].block_id = start_block_id;
        }
        Block& start_block = block_builders[start_block_id];
        start_block.adj_data.push_back(out_edge);
        start_block.offsets_out[start_local_id + 1]++;

        // 写入入边块
        if (!block_builders.count(end_block_id)) {
            block_builders[end_block_id] = Block();
            block_builders[end_block_id].block_id = end_block_id;
        }
        Block& end_block = block_builders[end_block_id];
        end_block.adj_data.push_back(in_edge);
        end_block.offsets_in[end_local_id + 1]++;
    }

    void ParseCSVRecordBatch(const std::vector<std::string>& lines, StringIdMapping& point_id_map,
                             LabelMapping& point_label_map, LabelMapping& edge_label_map,
                             std::unordered_map<uint32_t, Block>& block_builders) {
        for (const auto& line : lines) ParseCSVRecord(line, point_id_map, point_label_map, edge_label_map, block_builders);
    }

public:
    void LoadSingleCSVConcurrent(const std::string& csv_path, StringIdMapping& point_id_map,
                                 LabelMapping& point_label_map, LabelMapping& edge_label_map,
                                 BlockedCSR& csr, CacheManager& cache, uint64_t mem_limit_mb) {
        std::ifstream fin(csv_path);
        if (!fin) throw std::runtime_error("Open CSV failed: " + csv_path);

        std::string line;
        getline(fin, line);  // 跳过表头

        std::vector<std::string> batch_lines;
        const int BATCH_SIZE = 100000;
        int line_count = 0;

        std::vector<std::thread> threads;
        std::vector<std::unordered_map<uint32_t, Block>> thread_builders(3);
        std::atomic<int> thread_idx = 0;

        while (getline(fin, line)) {
            batch_lines.push_back(line);
            line_count++;

            if (batch_lines.size() >= BATCH_SIZE) {
                uint64_t current_mem = GetUsedMemory() / 1024 / 1024;
                if (current_mem >= mem_limit_mb * 0.9) {
                    for (auto& builder : thread_builders) {
                        for (auto& [bid, block] : builder) {
                            csr.AddBlock(block, false);
                            block.adj_data.clear();
                            block.adj_data.shrink_to_fit();
                        }
                    }
                    cache.ShrinkHotCache(0.5);
                    malloc_trim(0);
                }

                int idx = thread_idx.fetch_add(1) % 3;
                threads.emplace_back(&CSVLoader::ParseCSVRecordBatch, this, batch_lines,
                                     std::ref(point_id_map), std::ref(point_label_map),
                                     std::ref(edge_label_map), std::ref(thread_builders[idx]));
                batch_lines.clear();
            }
        }

        if (!batch_lines.empty()) {
            int idx = thread_idx.fetch_add(1) % 3;
            threads.emplace_back(&CSVLoader::ParseCSVRecordBatch, this, batch_lines,
                                 std::ref(point_id_map), std::ref(point_label_map),
                                 std::ref(edge_label_map), std::ref(thread_builders[idx]));
        }

        for (auto& t : threads) t.join();

        std::unordered_map<uint32_t, Block> merged_blocks;
        for (auto& builder : thread_builders) {
            for (auto& [bid, block] : builder) {
                if (merged_blocks.count(bid)) {
                    merged_blocks[bid].adj_data.insert(merged_blocks[bid].adj_data.end(),
                                                       block.adj_data.begin(), block.adj_data.end());
                    for (int i = 0; i < block.offsets_out.size(); i++) {
                        merged_blocks[bid].offsets_out[i] += block.offsets_out[i];
                        merged_blocks[bid].offsets_in[i] += block.offsets_in[i];
                    }
                } else {
                    merged_blocks[bid] = block;
                }
            }
        }

        for (auto& [bid, block] : merged_blocks) {
            if (bid < 1000) cache.AddHotBlock(block);
            else csr.AddBlock(block, false);
        }
    }
};