// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_META_STORE
#define CEPH_LIBRBD_CACHE_FILE_META_STORE

#include "include/int_types.h"
#include "librbd/cache/file/SyncFile.h"
#include "common/Mutex.h"

struct Context;

namespace librbd {

struct ImageCtx;

namespace cache {
namespace file {

template <typename ImageCtxT>
class MetaStore {
public:
  MetaStore(ImageCtxT &image_ctx, uint32_t block_size);

  void init(Context *on_finish);
  void shut_down(Context *on_finish);

  inline uint64_t offset_to_block(uint64_t offset) {
    return offset / m_block_size;
  }

  inline uint64_t block_to_offset(uint64_t block) {
    return block * m_block_size;
  }

private:
  ImageCtxT &m_image_ctx;
  uint32_t m_block_size;

  SyncFile<ImageCtx> m_aio_file;

};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::MetaStore<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_META_STORE
