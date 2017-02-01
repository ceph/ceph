// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SnapshotCreateRequest.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_sync::SnapshotCreateRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_sync {

using librbd::util::create_context_callback;
using librbd::util::create_rados_safe_callback;

template <typename I>
SnapshotCreateRequest<I>::SnapshotCreateRequest(I *local_image_ctx,
                                                const std::string &snap_name,
                                                uint64_t size,
                                                const librbd::parent_spec &spec,
                                                uint64_t parent_overlap,
                                                Context *on_finish)
  : m_local_image_ctx(local_image_ctx), m_snap_name(snap_name), m_size(size),
    m_parent_spec(spec), m_parent_overlap(parent_overlap),
    m_on_finish(on_finish) {
}

template <typename I>
void SnapshotCreateRequest<I>::send() {
  send_set_size();
}

template <typename I>
void SnapshotCreateRequest<I>::send_set_size() {
  m_local_image_ctx->snap_lock.get_read();
  if (m_local_image_ctx->size == m_size) {
    m_local_image_ctx->snap_lock.put_read();
    send_remove_parent();
    return;
  }
  m_local_image_ctx->snap_lock.put_read();

  dout(20) << dendl;

  // Change the image size on disk so that the snapshot picks up
  // the expected size.  We can do this because the last snapshot
  // we process is the sync snapshot which was created to match the
  // image size. We also don't need to worry about trimming because
  // we track the highest possible object number within the sync record
  librados::ObjectWriteOperation op;
  librbd::cls_client::set_size(&op, m_size);

  librados::AioCompletion *comp = create_rados_safe_callback<
    SnapshotCreateRequest<I>, &SnapshotCreateRequest<I>::handle_set_size>(this);
  int r = m_local_image_ctx->md_ctx.aio_operate(m_local_image_ctx->header_oid,
                                                comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void SnapshotCreateRequest<I>::handle_set_size(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update image size '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  {
    // adjust in-memory image size now that it's updated on disk
    RWLock::WLocker snap_locker(m_local_image_ctx->snap_lock);
    m_local_image_ctx->size = m_size;
  }

  send_remove_parent();
}

template <typename I>
void SnapshotCreateRequest<I>::send_remove_parent() {
  m_local_image_ctx->parent_lock.get_read();
  if (m_local_image_ctx->parent_md.spec.pool_id == -1 ||
      m_local_image_ctx->parent_md.spec == m_parent_spec) {
    m_local_image_ctx->parent_lock.put_read();
    send_set_parent();
    return;
  }
  m_local_image_ctx->parent_lock.put_read();

  dout(20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::remove_parent(&op);

  librados::AioCompletion *comp = create_rados_safe_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_remove_parent>(this);
  int r = m_local_image_ctx->md_ctx.aio_operate(m_local_image_ctx->header_oid,
                                                comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void SnapshotCreateRequest<I>::handle_remove_parent(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to remove parent '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  {
    // adjust in-memory parent now that it's updated on disk
    RWLock::WLocker parent_locker(m_local_image_ctx->parent_lock);
    m_local_image_ctx->parent_md.spec = {};
    m_local_image_ctx->parent_md.overlap = 0;
  }

  send_set_parent();
}

template <typename I>
void SnapshotCreateRequest<I>::send_set_parent() {
  m_local_image_ctx->parent_lock.get_read();
  if (m_local_image_ctx->parent_md.spec == m_parent_spec &&
      m_local_image_ctx->parent_md.overlap == m_parent_overlap) {
    m_local_image_ctx->parent_lock.put_read();
    send_snap_create();
    return;
  }
  m_local_image_ctx->parent_lock.put_read();

  dout(20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::set_parent(&op, m_parent_spec, m_parent_overlap);

  librados::AioCompletion *comp = create_rados_safe_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_set_parent>(this);
  int r = m_local_image_ctx->md_ctx.aio_operate(m_local_image_ctx->header_oid,
                                                comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void SnapshotCreateRequest<I>::handle_set_parent(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to set parent '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  {
    // adjust in-memory parent now that it's updated on disk
    RWLock::WLocker parent_locker(m_local_image_ctx->parent_lock);
    m_local_image_ctx->parent_md.spec = m_parent_spec;
    m_local_image_ctx->parent_md.overlap = m_parent_overlap;
  }

  send_snap_create();
}

template <typename I>
void SnapshotCreateRequest<I>::send_snap_create() {
  dout(20) << ": snap_name=" << m_snap_name << dendl;

  Context *ctx = create_context_callback<
    SnapshotCreateRequest<I>, &SnapshotCreateRequest<I>::handle_snap_create>(
      this);
  RWLock::RLocker owner_locker(m_local_image_ctx->owner_lock);
  m_local_image_ctx->operations->execute_snap_create(m_snap_name.c_str(), ctx,
                                                     0U, true);
}

template <typename I>
void SnapshotCreateRequest<I>::handle_snap_create(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create snapshot '" << m_snap_name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_create_object_map();
}
template <typename I>
void SnapshotCreateRequest<I>::send_create_object_map() {

  if (!m_local_image_ctx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    finish(0);
    return;
  }

  m_local_image_ctx->snap_lock.get_read();
  auto snap_it = m_local_image_ctx->snap_ids.find(m_snap_name);
  if (snap_it == m_local_image_ctx->snap_ids.end()) {
    derr << ": failed to locate snap: " << m_snap_name << dendl;
    m_local_image_ctx->snap_lock.put_read();
    finish(-ENOENT);
    return;
  }
  librados::snap_t local_snap_id = snap_it->second;
  m_local_image_ctx->snap_lock.put_read();

  std::string object_map_oid(librbd::ObjectMap<>::object_map_name(
    m_local_image_ctx->id, local_snap_id));
  uint64_t object_count = Striper::get_num_objects(m_local_image_ctx->layout,
                                                   m_size);
  dout(20) << ": "
           << "object_map_oid=" << object_map_oid << ", "
           << "object_count=" << object_count << dendl;

  // initialize an empty object map of the correct size (object sync
  // will populate the object map)
  librados::ObjectWriteOperation op;
  librbd::cls_client::object_map_resize(&op, object_count, OBJECT_NONEXISTENT);

  librados::AioCompletion *comp = create_rados_safe_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_create_object_map>(this);
  int r = m_local_image_ctx->md_ctx.aio_operate(object_map_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void SnapshotCreateRequest<I>::handle_create_object_map(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create object map: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void SnapshotCreateRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_sync::SnapshotCreateRequest<librbd::ImageCtx>;
