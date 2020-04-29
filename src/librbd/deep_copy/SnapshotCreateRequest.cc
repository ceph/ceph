// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SetHeadRequest.h"
#include "SnapshotCreateRequest.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::SnapshotCreateRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace deep_copy {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
SnapshotCreateRequest<I>::SnapshotCreateRequest(
    I *dst_image_ctx, const std::string &snap_name,
    const cls::rbd::SnapshotNamespace &snap_namespace,
    uint64_t size, const cls::rbd::ParentImageSpec &spec,
    uint64_t parent_overlap, Context *on_finish)
  : m_dst_image_ctx(dst_image_ctx), m_snap_name(snap_name),
    m_snap_namespace(snap_namespace), m_size(size),
    m_parent_spec(spec), m_parent_overlap(parent_overlap),
    m_on_finish(on_finish), m_cct(dst_image_ctx->cct) {
}

template <typename I>
void SnapshotCreateRequest<I>::send() {
  send_set_head();
}

template <typename I>
void SnapshotCreateRequest<I>::send_set_head() {
  ldout(m_cct, 20) << dendl;

  auto ctx = create_context_callback<
    SnapshotCreateRequest<I>, &SnapshotCreateRequest<I>::handle_set_head>(this);
  auto req = SetHeadRequest<I>::create(m_dst_image_ctx, m_size, m_parent_spec,
                                       m_parent_overlap, ctx);
  req->send();
}

template <typename I>
void SnapshotCreateRequest<I>::handle_set_head(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to set head: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_create_snap();
}

template <typename I>
void SnapshotCreateRequest<I>::send_create_snap() {
  ldout(m_cct, 20) << "snap_name=" << m_snap_name << dendl;

  int r;
  auto finish_op_ctx = start_lock_op(&r);
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(r);
    return;
  }

  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
      handle_create_snap(r);
      finish_op_ctx->complete(0);
    });
  std::shared_lock owner_locker{m_dst_image_ctx->owner_lock};
  m_dst_image_ctx->operations->execute_snap_create(m_snap_namespace,
                                                   m_snap_name.c_str(), ctx, 0U,
                                                   true, m_prog_ctx);
}

template <typename I>
void SnapshotCreateRequest<I>::handle_create_snap(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create snapshot '" << m_snap_name << "': "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_create_object_map();
}
template <typename I>
void SnapshotCreateRequest<I>::send_create_object_map() {

  if (!m_dst_image_ctx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    finish(0);
    return;
  }

  m_dst_image_ctx->image_lock.lock_shared();
  auto snap_it = m_dst_image_ctx->snap_ids.find(
    {cls::rbd::UserSnapshotNamespace(), m_snap_name});
  if (snap_it == m_dst_image_ctx->snap_ids.end()) {
    lderr(m_cct) << "failed to locate snap: " << m_snap_name << dendl;
    m_dst_image_ctx->image_lock.unlock_shared();
    finish(-ENOENT);
    return;
  }
  librados::snap_t local_snap_id = snap_it->second;
  m_dst_image_ctx->image_lock.unlock_shared();

  std::string object_map_oid(librbd::ObjectMap<>::object_map_name(
    m_dst_image_ctx->id, local_snap_id));
  uint64_t object_count = Striper::get_num_objects(m_dst_image_ctx->layout,
                                                   m_size);
  ldout(m_cct, 20) << "object_map_oid=" << object_map_oid << ", "
                   << "object_count=" << object_count << dendl;

  // initialize an empty object map of the correct size (object sync
  // will populate the object map)
  librados::ObjectWriteOperation op;
  librbd::cls_client::object_map_resize(&op, object_count, OBJECT_NONEXISTENT);

  int r;
  auto finish_op_ctx = start_lock_op(&r);
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(r);
    return;
  }

  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
      handle_create_object_map(r);
      finish_op_ctx->complete(0);
    });
  librados::AioCompletion *comp = create_rados_callback(ctx);
  r = m_dst_image_ctx->md_ctx.aio_operate(object_map_oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void SnapshotCreateRequest<I>::handle_create_object_map(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create object map: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
Context *SnapshotCreateRequest<I>::start_lock_op(int* r) {
  std::shared_lock owner_locker{m_dst_image_ctx->owner_lock};
  if (m_dst_image_ctx->exclusive_lock == nullptr) {
    return new LambdaContext([](int r) {});
  }
  return m_dst_image_ctx->exclusive_lock->start_op(r);
}

template <typename I>
void SnapshotCreateRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace deep_copy
} // namespace librbd

template class librbd::deep_copy::SnapshotCreateRequest<librbd::ImageCtx>;
