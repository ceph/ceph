// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/ResizeRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "cls/lock/cls_lock_client.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::ResizeRequest: "

namespace librbd {
namespace object_map {

void ResizeRequest::resize(ceph::BitVector<2> *object_map, uint64_t num_objs,
                           uint8_t default_state) {
  size_t orig_object_map_size = object_map->size();
  object_map->resize(num_objs);
  for (uint64_t i = orig_object_map_size; i < object_map->size(); ++i) {
    (*object_map)[i] = default_state;
  }
}

void ResizeRequest::send() {
  CephContext *cct = m_image_ctx.cct;

  RWLock::WLocker l(m_image_ctx.object_map_lock);
  m_num_objs = Striper::get_num_objects(m_image_ctx.layout, m_new_size);

  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 5) << this << " resizing on-disk object map: "
                << "ictx=" << &m_image_ctx << ", "
                << "oid=" << oid << ", num_objs=" << m_num_objs << dendl;

  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  }
  cls_client::object_map_resize(&op, m_num_objs, m_default_object_state);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

void ResizeRequest::finish_request() {
  RWLock::WLocker object_map_locker(m_image_ctx.object_map_lock);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " resizing in-memory object map: "
		<< m_num_objs << dendl;

  resize(m_object_map, m_num_objs, m_default_object_state);
}

} // namespace object_map
} // namespace librbd
