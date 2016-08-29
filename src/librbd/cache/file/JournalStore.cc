// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/JournalStore.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/file/MetaStore.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::file::JournalStore: " << this \
                           << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace file {

template <typename I>
JournalStore<I>::JournalStore(I &image_ctx, MetaStore<I> &metastore)
  : m_image_ctx(image_ctx), m_metastore(metastore),
    m_aio_file(image_ctx, *image_ctx.op_work_queue, image_ctx.id + ".journal") {
}

template <typename I>
void JournalStore<I>::init(Context *on_finish) {
  // TODO
  on_finish->complete(0);
}

template <typename I>
void JournalStore<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  on_finish->complete(0);
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::JournalStore<librbd::ImageCtx>;
