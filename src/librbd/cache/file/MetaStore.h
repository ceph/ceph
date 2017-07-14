// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_META_STORE
#define CEPH_LIBRBD_CACHE_FILE_META_STORE

#include "include/int_types.h"
#include "os/CacheStore/SyncFile.h"
#include <mutex>

struct Context;

namespace librbd {

struct ImageCtx;

namespace cache {
namespace file {

template <typename ImageCtxT>
class MetaStore {
public:
  MetaStore(ImageCtxT &image_ctx, uint64_t block_count);

  void init(Context *on_finish);
  void remove(Context *on_finish);
  void shut_down(Context *on_finish);
  void update(uint64_t block_id, uint32_t loc);
  void get_loc_map(uint32_t *dest);
  void load(uint32_t loc);

private:
  ImageCtxT &m_image_ctx;
  uint64_t m_block_count;
  uint32_t *m_loc_map;
  //mutable Mutex m_lock;
  std::mutex m_lock;
  bool init_m_loc_map;

  os::CacheStore::SyncFile m_meta_file;

};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::MetaStore<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_META_STORE
