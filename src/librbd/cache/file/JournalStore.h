// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_JOURNAL_STORE
#define CEPH_LIBRBD_CACHE_FILE_JOURNAL_STORE

#include "librbd/cache/file/AioFile.h"

struct Context;

namespace librbd {

struct ImageCtx;

namespace cache {
namespace file {

template <typename> class MetaStore;

template <typename ImageCtxT>
class JournalStore {
public:
  JournalStore(ImageCtxT &image_ctx, MetaStore<ImageCtxT> &metastore);

  void init(Context *on_finish);
  void shut_down(Context *on_finish);

private:
  ImageCtxT &m_image_ctx;
  MetaStore<ImageCtxT> &m_metastore;
  AioFile<ImageCtx> m_aio_file;

};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::JournalStore<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_JOURNAL_STORE
