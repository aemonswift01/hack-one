/* XXH3 Hash Library
 * A fast, high-quality hash function
 */
#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

uint64_t XXH3_64bits(const void* data, size_t len);
uint64_t XXH3_64bits_withSeed(const void* data, size_t len, uint64_t seed);

#ifdef __cplusplus
}
#endif

/* Simple implementation for our use case */
static inline uint64_t XXH3(const void* data, size_t len) {
    return XXH3_64bits(data, len);
}