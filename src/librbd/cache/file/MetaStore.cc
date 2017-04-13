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
    m_meta_file(image_ctx, *image_ctx.op_work_queue, image_ctx.id + ".meta") {
}

template <typename I>
void MetaStore<I>::init(bufferlist *bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  Context *ctx = new FunctionContext(
    [this, bl, on_finish](int r) {
      if (r < 0) {
        on_finish->complete(r);
        return;
      }
      //check if exists? yes: load_all, no: truncate
      if(m_meta_file.filesize() == 0) {
        reset(on_finish);
      } else {
        load_all(bl, on_finish);
      }
    });
  m_meta_file.open(ctx);
}

template <typename I>
void MetaStore<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  m_meta_file.close(on_finish);
}

template <typename I>
void MetaStore<I>::reset(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  m_meta_file.truncate(offset_to_block(m_image_ctx.size) * m_entry_size, false, on_finish);
}

template <typename I>
void MetaStore<I>::set_entry_size(uint32_t entry_size) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  m_entry_size = entry_size;
}
template <typename I>
void MetaStore<I>::write_block(uint64_t cache_block, bufferlist bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  uint64_t meta_block_offset = cache_block * m_entry_size;
  m_meta_file.write(meta_block_offset, std::move(bl), true, on_finish);
}

template <typename I>
void MetaStore<I>::read_block(uint64_t cache_block, bufferlist *bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  uint64_t meta_block_offset = cache_block * m_entry_size;
  m_meta_file.read(meta_block_offset, m_entry_size, bl, on_finish);
}

template <typename I>
void MetaStore<I>::load_all(bufferlist *bl, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  //1. get total file length
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r < 0) {
        on_finish->complete(r);
        return;
      }
  });
  for(uint64_t block_id = 0; block_id < offset_to_block(m_image_ctx.size); block_id++){
    read_block(block_id, bl, ctx);
    if (bl->is_zero()) break;
  }
  on_finish->complete(0);
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::MetaStore<librbd::ImageCtx>;
