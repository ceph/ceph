// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/ImageStore.h"
#include "include/buffer.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/file/MetaStore.h"
#include "librbd/cache/file/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::file::ImageStore: " << this \
                           << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace file {

template <typename I>
ImageStore<I>::ImageStore(I &image_ctx, MetaStore<I> &metastore)
  : m_image_ctx(image_ctx), m_metastore(metastore),
    m_cache_file(image_ctx, *image_ctx.op_work_queue,
                 image_ctx.id + ".image_cache") {
}

template <typename I>
void ImageStore<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO don't reset the read-only cache
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r < 0) {
        on_finish->complete(r);
        return;
      }

      reset(on_finish);
    });
  m_cache_file.open(ctx);
}

template <typename I>
void ImageStore<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  m_cache_file.close(on_finish);
}

template <typename I>
void ImageStore<I>::reset(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  m_cache_file.truncate(m_image_ctx.size, false, on_finish);
}

template <typename I>
void ImageStore<I>::read_block(uint64_t cache_block,
                               BlockExtents &&block_extents,
                               bufferlist *bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << cache_block << ", "
                 << "extents=" << block_extents << dendl;

  // TODO add gather support
  assert(block_extents.size() == 1);
  auto &extent = block_extents.front();
  m_cache_file.read(m_metastore.block_to_offset(cache_block) + extent.first,
                    extent.second, bl, on_finish);
}

template <typename I>
void ImageStore<I>::write_block(uint64_t cache_block,
                                BlockExtents &&block_extents,
                                bufferlist &&bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block=" << cache_block << ", "
                 << "extents=" << block_extents << dendl;

  // TODO add scatter support
  C_Gather *ctx = new C_Gather(cct, on_finish);
  uint64_t buffer_offset = 0;
  for (auto &extent : block_extents) {
    bufferlist sub_bl;
    sub_bl.substr_of(bl, buffer_offset, extent.second);
    buffer_offset += extent.second;

    m_cache_file.write(m_metastore.block_to_offset(cache_block) + extent.first,
                       std::move(sub_bl), false, ctx->new_sub());

  }
  ctx->activate();
}

template <typename I>
void ImageStore<I>::discard_block(uint64_t cache_block, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  on_finish->complete(0);
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::ImageStore<librbd::ImageCtx>;
