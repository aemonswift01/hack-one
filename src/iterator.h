#pragma once
#include <cstdint>
#include <utility>
#include <string>

struct Iterator {
    virtual ~Iterator() = default;
    virtual bool HasNext() const = 0;
    virtual void Next() = 0;
    virtual void Reset() = 0;
};

struct AdjIterator : public Iterator {
    virtual const std::pair<uint32_t, uint32_t>* GetValuePtr() const = 0;
};

struct StringIdToIntIdIter : public Iterator {
    virtual std::pair<std::string, uint32_t> GetValue() const = 0;
};