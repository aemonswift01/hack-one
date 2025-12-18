#include "../include/MmappedCSR.hpp"
#include <fstream>
#include <filesystem>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

MmappedCSR::~MmappedCSR() {
    if (offsets_) munmap((void*)offsets_, mapped_size_offsets_);
    if (neighbors_) munmap((void*)neighbors_, mapped_size_neighbors_);
    if (edge_labels_) munmap((void*)edge_labels_, mapped_size_labels_);
}

void MmappedCSR::load(const std::string& prefix) {
    std::string offsets_path = prefix + "_offsets.bin";
    std::string neighbors_path = prefix + "_neighbors.bin";
    std::string labels_path = prefix + "_edge_labels.bin";
    
    // Map offsets
    int fd_offsets = open(offsets_path.c_str(), O_RDONLY);
    if (fd_offsets == -1) throw std::runtime_error("Failed to open " + offsets_path);
    
    struct stat st_offsets;
    if (fstat(fd_offsets, &st_offsets) == -1) {
        close(fd_offsets);
        throw std::runtime_error("Failed to stat " + offsets_path);
    }
    
    mapped_size_offsets_ = st_offsets.st_size;
    num_nodes_ = (mapped_size_offsets_ / sizeof(uint32_t)) - 1;
    
    offsets_ = (const uint32_t*)mmap(nullptr, mapped_size_offsets_, PROT_READ, MAP_PRIVATE, fd_offsets, 0);
    close(fd_offsets);
    
    if (offsets_ == MAP_FAILED) {
        throw std::runtime_error("Failed to mmap " + offsets_path);
    }
    
    // Map neighbors
    int fd_neighbors = open(neighbors_path.c_str(), O_RDONLY);
    if (fd_neighbors == -1) {
        munmap((void*)offsets_, mapped_size_offsets_);
        offsets_ = nullptr;
        throw std::runtime_error("Failed to open " + neighbors_path);
    }
    
    struct stat st_neighbors;
    if (fstat(fd_neighbors, &st_neighbors) == -1) {
        close(fd_neighbors);
        munmap((void*)offsets_, mapped_size_offsets_);
        offsets_ = nullptr;
        throw std::runtime_error("Failed to stat " + neighbors_path);
    }
    
    mapped_size_neighbors_ = st_neighbors.st_size;
    
    neighbors_ = (const uint32_t*)mmap(nullptr, mapped_size_neighbors_, PROT_READ, MAP_PRIVATE, fd_neighbors, 0);
    close(fd_neighbors);
    
    if (neighbors_ == MAP_FAILED) {
        munmap((void*)offsets_, mapped_size_offsets_);
        offsets_ = nullptr;
        throw std::runtime_error("Failed to mmap " + neighbors_path);
    }
    
    // Map edge labels
    int fd_labels = open(labels_path.c_str(), O_RDONLY);
    if (fd_labels == -1) {
        munmap((void*)offsets_, mapped_size_offsets_);
        munmap((void*)neighbors_, mapped_size_neighbors_);
        offsets_ = nullptr;
        neighbors_ = nullptr;
        throw std::runtime_error("Failed to open " + labels_path);
    }
    
    struct stat st_labels;
    if (fstat(fd_labels, &st_labels) == -1) {
        close(fd_labels);
        munmap((void*)offsets_, mapped_size_offsets_);
        munmap((void*)neighbors_, mapped_size_neighbors_);
        offsets_ = nullptr;
        neighbors_ = nullptr;
        throw std::runtime_error("Failed to stat " + labels_path);
    }
    
    mapped_size_labels_ = st_labels.st_size;
    
    edge_labels_ = (const uint8_t*)mmap(nullptr, mapped_size_labels_, PROT_READ, MAP_PRIVATE, fd_labels, 0);
    close(fd_labels);
    
    if (edge_labels_ == MAP_FAILED) {
        munmap((void*)offsets_, mapped_size_offsets_);
        munmap((void*)neighbors_, mapped_size_neighbors_);
        offsets_ = nullptr;
        neighbors_ = nullptr;
        throw std::runtime_error("Failed to mmap " + labels_path);
    }
}