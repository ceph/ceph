// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_IMAGE_STORE
#define CEPH_LIBRBD_CACHE_FILE_IMAGE_STORE

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "os/CacheStore/SyncFile.h"
#include "librbd/cache/file/MetaStore.cc"
#include <vector>

struct Context;

namespace librbd {

struct ImageCtx;

namespace cache {
namespace file {

template <typename> class MetaStore;

template <typename ImageCtxT>
class ImageStore {
public:
  typedef std::vector<std::pair<uint32_t, uint32_t> > BlockExtents;

  ImageStore(ImageCtxT &image_ctx, MetaStore<ImageCtxT> &metastore);

  void init(Context *on_finish);
  void shut_down(Context *on_finish);
  void reset(Context *on_finish);

  void read_block(uint64_t cache_block, BlockExtents &&block_extents,
                  ceph::bufferlist *bl, Context *on_finish);
  void write_block(uint64_t cache_block, BlockExtents &&block_extents,
                   ceph::bufferlist &&bl, Context *on_finish);
  void discard_block(uint64_t cache_block, Context *on_finish);

private:
  ImageCtxT &m_image_ctx;
  MetaStore<ImageCtxT> &m_metastore;
  os::CacheStore::SyncFile m_cache_file;

};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::ImageStore<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_IMAGE_STORE
