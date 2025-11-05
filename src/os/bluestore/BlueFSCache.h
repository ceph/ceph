#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <unordered_map>
#include "bluestore_common.h"
#include "include/buffer_fwd.h"
#include "tools/cephfs/EventOutput.h"


using cache_bufferlist = std::shared_ptr<bufferlist>;
// Hash function for std::pair<uint64_t, uint64_t>
namespace std {
    template <>
    struct hash<std::pair<uint64_t, uint64_t>> {
        size_t operator()(const std::pair<uint64_t, uint64_t>& p) const {
            return std::hash<uint64_t>()(p.first) ^ (std::hash<uint64_t>()(p.second) << 1);
        }
    };
}

class LRUCache
{
public:
    LRUCache(size_t cache_capacity, size_t evict_bytes, CephContext* cct);

    /*
    bufferlist* get(uint64_t inode, uint64_t offset)
    {
        std::shared_lock lock(mutex);
        auto it = kv.find({inode, offset});
        if (it == kv.end())
        {
            return nullptr;
        }

        updateRecency({inode, offset});
        return it->second;
    }*/

    cache_bufferlist get_nearest_offset(uint64_t inode, uint64_t offset, uint64_t *buffer_offset);

    int put(uint64_t inode, uint64_t offset, cache_bufferlist value);

    void remove(std::pair<uint64_t, uint64_t> key);

    bool is_empty();

    cache_bufferlist create_bufferlist();

private:
    size_t cache_capacity;
    size_t evict_bytes;
    size_t current_bytes;
    CephContext* cct;

    using inode_offset_pair = std::pair<uint64_t, uint64_t>;

    std::map<inode_offset_pair,cache_bufferlist> kv;                                   // key -> value
    std::shared_mutex mutex;

    std::list<inode_offset_pair> access_order;                                     // most recent at front
    std::unordered_map<inode_offset_pair, std::list<inode_offset_pair>::iterator> key_iter; // key -> position in the list
    std::shared_mutex mutex_iter;

    void print_cache();

    void update_recency(std::pair<uint64_t, uint64_t> key);

    void evict(size_t target_free_bytes);
};