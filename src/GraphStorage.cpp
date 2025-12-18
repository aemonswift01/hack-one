#include "../include/GraphStorage.hpp"
#include <fstream>
#include <filesystem>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

void GraphStorage::load(const std::string& dir) {
    // Load CSR structures
    out_csr_.load(dir + "/out");
    in_csr_.load(dir + "/in");
    
    // Load ID mapper
    id_mapper_.load(dir);
    
    // Load meta data
    std::string meta_path = dir + "/meta.bin";
    std::ifstream meta_file(meta_path, std::ios::binary);
    if (!meta_file) {
        throw std::runtime_error("Failed to open " + meta_path);
    }
    
    meta_file.read(reinterpret_cast<char*>(&num_nodes_), sizeof(num_nodes_));
    meta_file.read(reinterpret_cast<char*>(&num_edges_), sizeof(num_edges_));
    
    // Load node labels
    std::string node_labels_path = dir + "/node_labels.bin";
    int fd_node_labels = open(node_labels_path.c_str(), O_RDONLY);
    if (fd_node_labels == -1) {
        throw std::runtime_error("Failed to open " + node_labels_path);
    }
    
    struct stat st_node_labels;
    if (fstat(fd_node_labels, &st_node_labels) == -1) {
        close(fd_node_labels);
        throw std::runtime_error("Failed to stat " + node_labels_path);
    }
    
    node_labels_mapped_size_ = st_node_labels.st_size;
    node_labels_ = (uint8_t*)mmap(nullptr, node_labels_mapped_size_, PROT_READ, MAP_PRIVATE, fd_node_labels, 0);
    close(fd_node_labels);
    
    if (node_labels_ == MAP_FAILED) {
        node_labels_ = nullptr;
        throw std::runtime_error("Failed to mmap " + node_labels_path);
    }
    
    // Load node label strings
    std::string node_label_strings_path = dir + "/node_label_strings.txt";
    std::ifstream node_label_strings_file(node_label_strings_path);
    if (!node_label_strings_file) {
        throw std::runtime_error("Failed to open " + node_label_strings_path);
    }
    
    std::string line;
    while (std::getline(node_label_strings_file, line)) {
        node_label_strings_.push_back(line);
    }
    
    // Load edge label strings
    std::string edge_label_strings_path = dir + "/edge_label_strings.txt";
    std::ifstream edge_label_strings_file(edge_label_strings_path);
    if (!edge_label_strings_file) {
        throw std::runtime_error("Failed to open " + edge_label_strings_path);
    }
    
    while (std::getline(edge_label_strings_file, line)) {
        edge_label_strings_.push_back(line);
    }
}