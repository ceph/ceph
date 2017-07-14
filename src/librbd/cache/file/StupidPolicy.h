// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY
#define CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY

#include "librbd/cache/Block.h"
#include "librbd/cache/file/Policy.h"
#include "common/Mutex.h"
#include <unordered_map>
#include <vector>

namespace librbd {

struct ImageCtx;

namespace cache {

namespace file {

/**
 * Stupid LRU-style policy
 */
template <typename ImageCtxT>
class StupidPolicy : public Policy {
private:
  typedef std::vector<Entry> Entries;

  ImageCtxT &m_image_ctx;
  mutable Mutex m_lock;
  Entries m_entries;
  BlockMap* m_block_map;
  uint64_t m_block_count;

  LRUList m_free_lru;
  LRUList m_clean_lru;

public:
  StupidPolicy(ImageCtxT &image_ctx);

  virtual void set_block_count(uint64_t block_count);

  virtual int invalidate(uint64_t block);

  virtual int map(IOType io_type, uint64_t block, bool partial_block,
                  PolicyMapResult *policy_map_result);
  virtual void tick();
  void set_to_base_cache(uint64_t block);
  uint64_t block_to_offset(uint64_t block) override;

  virtual uint32_t get_loc(uint64_t block);
  virtual void set_loc(uint32_t *src);
  inline uint64_t get_block_count() {
    return m_block_count;
  }
  inline void* get_block_map() {
    return m_block_map;
  }
};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::StupidPolicy<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY
