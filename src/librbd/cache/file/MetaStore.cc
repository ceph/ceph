// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/MetaStore.h"
#include "include/stringify.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include <string>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::file::MetaStore: " << this \
                           << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace file {

template <typename I>
MetaStore<I>::MetaStore(I &image_ctx, uint32_t block_size)
  : m_image_ctx(image_ctx), m_block_size(block_size),
    m_aio_file(image_ctx, *image_ctx.op_work_queue, image_ctx.id + ".meta") {
}

template <typename I>
void MetaStore<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  m_aio_file.open(on_finish);
}

template <typename I>
void MetaStore<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  m_aio_file.close(on_finish);
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::MetaStore<librbd::ImageCtx>;
