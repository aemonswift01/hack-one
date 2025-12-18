#include "IdMapper.hpp"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
#include <fstream>

#include "xxhash.h"

IdMapper::~IdMapper() {
    if (hashes_) {
        munmap((void*)hashes_, mapped_size_);
    }
}

void IdMapper::load(const std::string& dir) {
    std::string hashes_path = dir + "/id_hashes.bin";
    std::string ids_path = dir + "/id_internal_ids.bin";

    // Map hashes file
    int fd_hashes = open(hashes_path.c_str(), O_RDONLY);
    if (fd_hashes == -1)
        throw std::runtime_error("Failed to open " + hashes_path);

    struct stat st;
    if (fstat(fd_hashes, &st) == -1) {
        close(fd_hashes);
        throw std::runtime_error("Failed to stat " + hashes_path);
    }

    mapped_size_ = st.st_size;
    num_nodes_ = mapped_size_ / sizeof(uint64_t);

    hashes_ = (const uint64_t*)mmap(nullptr, mapped_size_, PROT_READ,
                                    MAP_PRIVATE, fd_hashes, 0);
    close(fd_hashes);

    if (hashes_ == MAP_FAILED) {
        throw std::runtime_error("Failed to mmap " + hashes_path);
    }

    // Map ids file
    int fd_ids = open(ids_path.c_str(), O_RDONLY);
    if (fd_ids == -1)
        throw std::runtime_error("Failed to open " + ids_path);

    ids_ = (const uint32_t*)mmap(nullptr, mapped_size_, PROT_READ, MAP_PRIVATE,
                                 fd_ids, 0);
    close(fd_ids);

    if (ids_ == MAP_FAILED) {
        munmap((void*)hashes_, mapped_size_);
        hashes_ = nullptr;
        throw std::runtime_error("Failed to mmap " + ids_path);
    }
}

std::optional<uint32_t> IdMapper::external_id_to_internal(
    std::string_view external_id) const {
    uint64_t hash = hash_string(external_id);

    // Binary search
    size_t left = 0, right = num_nodes_ - 1;
    while (left <= right) {
        size_t mid = left + (right - left) / 2;
        if (hashes_[mid] == hash) {
            return ids_[mid];
        } else if (hashes_[mid] < hash) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return std::nullopt;
}

uint64_t IdMapper::hash_string(std::string_view s) {
    return XXH32(s.data(), s.size(), 0);
}