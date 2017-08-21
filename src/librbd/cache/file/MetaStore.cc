// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/MetaStore.h"
#include "librbd/cache/Types.h"
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
MetaStore<I>::MetaStore(I &image_ctx, uint64_t block_count)
  : m_image_ctx(image_ctx), m_block_count(block_count),
    m_meta_file(image_ctx.cct, *image_ctx.op_work_queue, image_ctx.id + ".meta"){
}

template <typename I>
bool MetaStore<I>::check_exists() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  if (!m_meta_file.try_open()) {
    return false;
  }
  return true;
}

template <typename I>
void MetaStore<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  Context* ctx;

  ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r < 0) {
        on_finish->complete(r);
      } else {
        assert(m_meta_file.load((void**)&m_loc_map, m_block_count * sizeof(uint32_t)) == 0);
        on_finish->complete(r);
      }
  });
  m_meta_file.open(ctx);
}

template <typename I>
void MetaStore<I>::remove(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  m_meta_file.remove(on_finish);
}

template <typename I>
void MetaStore<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO
  m_meta_file.close(on_finish);
}

template <typename I>
void MetaStore<I>::load(uint32_t loc) {
  uint32_t tmp;
  for(uint64_t block = 0; block < m_block_count; block++) {
    switch(loc) {
      case NOT_IN_CACHE:
        m_loc_map[block] = (loc << 30);
        break;
      case LOCATE_IN_BASE_CACHE:
        tmp = loc << 30;
        m_loc_map[block] = tmp;
        break;
      default:
        assert(0);
    }
  }
}

template <typename I>
void MetaStore<I>::update(uint64_t block_id, uint32_t loc) {
  //Mutex::Locker locker(m_lock);
  std::lock_guard<std::mutex> lock(m_lock);
  m_loc_map[block_id] = loc;
}

template <typename I>
void MetaStore<I>::get_loc_map(uint32_t* dest) {
  for(uint64_t block = 0; block < m_block_count; block++) {
    dest[block] = m_loc_map[block];
  }
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::MetaStore<librbd::ImageCtx>;
