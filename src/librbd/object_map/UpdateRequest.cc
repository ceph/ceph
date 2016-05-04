// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/UpdateRequest.h"
#include "include/rbd/object_map_types.h"
#include "include/stringify.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "cls/lock/cls_lock_client.h"
#include <string>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::UpdateRequest: "

namespace librbd {
namespace object_map {

void UpdateRequest::send() {
  assert(m_image_ctx.snap_lock.is_locked());
  assert(m_image_ctx.object_map_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;

  // safe to update in-memory state first without handling rollback since any
  // failures will invalidate the object map
  std::string oid(ObjectMap::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 20) << this << " updating object map"
                 << ": ictx=" << &m_image_ctx << ", oid=" << oid << ", ["
		 << m_start_object_no << "," << m_end_object_no << ") = "
		 << (m_current_state ?
		       stringify(static_cast<uint32_t>(*m_current_state)) : "")
		 << "->" << static_cast<uint32_t>(m_new_state)
		 << dendl;

  // rebuilding the object map might update on-disk only
  if (m_snap_id == m_image_ctx.snap_id) {
    assert(m_image_ctx.object_map_lock.is_wlocked());
    for (uint64_t object_no = m_start_object_no;
         object_no < MIN(m_end_object_no, m_object_map.size());
         ++object_no) {
      uint8_t state = m_object_map[object_no];
      if (!m_current_state || state == *m_current_state ||
          (*m_current_state == OBJECT_EXISTS && state == OBJECT_EXISTS_CLEAN)) {
        m_object_map[object_no] = m_new_state;
      }
    }
  }

  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  }
  cls_client::object_map_update(&op, m_start_object_no, m_end_object_no,
				m_new_state, m_current_state);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

void UpdateRequest::finish_request() {
  ldout(m_image_ctx.cct, 20) << this << " on-disk object map updated"
                             << dendl;
}

} // namespace object_map
} // namespace librbd
