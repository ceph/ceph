// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/UpdateRequest.h"
#include "librbd/object_map/BatchUpdateRequest.h"
#include "common/dout.h"
#include "common/WorkQueue.h"
#include "librbd/Utils.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::BatchUpdateRequest: " \
                           << this << " " << __func__

using librbd::ObjectMapView;

namespace librbd {

using util::create_context_callback;

namespace object_map {

template<typename I>
BatchUpdateRequest<I>::BatchUpdateRequest(I &image_ctx, ObjectMapView *object_map,
                                          uint64_t snap_id, Context *on_finish)
  : m_image_ctx(image_ctx), m_object_map(object_map), m_snap_id(snap_id),
    m_on_finish(on_finish) {
  m_cct = m_image_ctx.cct;
}

template<typename I>
void BatchUpdateRequest<I>::send() {
  ldout(m_cct, 20) << dendl;

  // get a copy which we can mod
  m_object_map_dup = *m_object_map;
  // apply views to the master ObjectMap
  m_object_map->apply_view();

  // switch thread context to acquire locks
  switch_thread_context();
}

template<typename I>
void BatchUpdateRequest<I>::switch_thread_context() {
  ldout(m_cct, 20) << dendl;

  using klass = BatchUpdateRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_swtich_thread_context>(this);

  m_image_ctx.op_work_queue->queue(ctx, 0);
}

template<typename I>
void BatchUpdateRequest<I>::handle_swtich_thread_context(int r) {
  ldout(m_cct, 20) << ": r=" << r << dendl;

  update_object_map();
}

template<typename I>
void BatchUpdateRequest<I>::update_object_map() {
  ldout(m_cct, 20) << dendl;

  if (m_view_idx == OBJECT_MAP_VIEW_LEVELS) {
    finish();
    return;
  }

  ldout(m_cct, 20) << ": flushing object map view (idx: "
                   << static_cast<uint32_t>(m_view_idx) << ")" << dendl;

  using klass = BatchUpdateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_update_object_map>(this);

  m_object_map_dup.sync_view(m_image_ctx, m_snap_id, m_view_idx, ctx);
}

template<typename I>
Context *BatchUpdateRequest<I>::handle_update_object_map(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    return m_on_finish; // object map invalidation is taken care by UpdateRequest
  }

  // update object map for next view
  ++m_view_idx;
  update_object_map();
  return nullptr;
}

template<typename I>
void BatchUpdateRequest<I>::finish() {
  ldout(m_cct, 20) << dendl;

  m_on_finish->complete(0);
  delete this;
}

} // namespace object_map
} // namespace librbd

template class librbd::object_map::BatchUpdateRequest<librbd::ImageCtx>;
