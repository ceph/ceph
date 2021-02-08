// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/UpdateRequest.h"
#include "include/rbd/object_map_types.h"
#include "include/stringify.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "cls/lock/cls_lock_client.h"
#include <string>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::UpdateRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace object_map {

namespace {

// keep aligned to bit_vector 4K block sizes
const uint64_t MAX_OBJECTS_PER_UPDATE = 256 * (1 << 10);

}

template <typename I>
void UpdateRequest<I>::send() {
  update_object_map();
}

template <typename I>
void UpdateRequest<I>::update_object_map() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  ceph_assert(ceph_mutex_is_locked(*m_object_map_lock));
  CephContext *cct = m_image_ctx.cct;

  // break very large requests into manageable batches
  m_update_end_object_no = std::min(
    m_end_object_no, m_update_start_object_no + MAX_OBJECTS_PER_UPDATE);

  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", oid=" << oid << ", "
                 << "[" << m_update_start_object_no << ","
                        << m_update_end_object_no << ") = "
		 << (m_current_state ?
		       stringify(static_cast<uint32_t>(*m_current_state)) : "")
		 << "->" << static_cast<uint32_t>(m_new_state)
		 << dendl;

  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, ClsLockType::EXCLUSIVE, "", "");
  }
  cls_client::object_map_update(&op, m_update_start_object_no,
                                m_update_end_object_no, m_new_state,
                                m_current_state);

  auto rados_completion = librbd::util::create_rados_callback<
    UpdateRequest<I>, &UpdateRequest<I>::handle_update_object_map>(this);
  std::vector<librados::snap_t> snaps;
  int r = m_image_ctx.md_ctx.aio_operate(
    oid, rados_completion, &op, 0, snaps,
    (m_trace.valid() ? m_trace.get_info() : nullptr));
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
void UpdateRequest<I>::handle_update_object_map(int r) {
  ldout(m_image_ctx.cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT && m_ignore_enoent) {
    r = 0;
  }
  if (r < 0 && m_ret_val == 0) {
    m_ret_val = r;
  }

  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    std::unique_lock object_map_locker{*m_object_map_lock};
    update_in_memory_object_map();

    if (m_update_end_object_no < m_end_object_no) {
      m_update_start_object_no = m_update_end_object_no;
      update_object_map();
      return;
    }
  }

  // no more batch updates to send
  complete(m_ret_val);
}

template <typename I>
void UpdateRequest<I>::update_in_memory_object_map() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  ceph_assert(ceph_mutex_is_locked(*m_object_map_lock));

  // rebuilding the object map might update on-disk only
  if (m_snap_id == m_image_ctx.snap_id) {
    ldout(m_image_ctx.cct, 20) << dendl;

    auto it = m_object_map.begin() +
      std::min(m_update_start_object_no, m_object_map.size());
    auto end_it = m_object_map.begin() +
      std::min(m_update_end_object_no, m_object_map.size());
    for (; it != end_it; ++it) {
      auto state_ref = *it;
      uint8_t state = state_ref;
      if (!m_current_state || state == *m_current_state ||
          (*m_current_state == OBJECT_EXISTS && state == OBJECT_EXISTS_CLEAN)) {
        state_ref = m_new_state;
      }
    }
  }
}

template <typename I>
void UpdateRequest<I>::finish_request() {
}

} // namespace object_map
} // namespace librbd

template class librbd::object_map::UpdateRequest<librbd::ImageCtx>;
