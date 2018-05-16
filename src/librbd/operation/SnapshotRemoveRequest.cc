// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotRemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/image/DetachChildRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotRemoveRequest: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace operation {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
SnapshotRemoveRequest<I>::SnapshotRemoveRequest(
    I &image_ctx, Context *on_finish,
    const cls::rbd::SnapshotNamespace &snap_namespace,
    const std::string &snap_name, uint64_t snap_id)
  : Request<I>(image_ctx, on_finish), m_snap_namespace(snap_namespace),
    m_snap_name(snap_name), m_snap_id(snap_id) {
}

template <typename I>
void SnapshotRemoveRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  assert(image_ctx.owner_lock.is_locked());
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    RWLock::RLocker object_map_locker(image_ctx.object_map_lock);
    if (image_ctx.snap_info.find(m_snap_id) == image_ctx.snap_info.end()) {
      lderr(cct) << "snapshot doesn't exist" << dendl;
      this->async_complete(-ENOENT);
      return;
    }
  }

  trash_snap();
}

template <typename I>
bool SnapshotRemoveRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void SnapshotRemoveRequest<I>::trash_snap() {
  I &image_ctx = this->m_image_ctx;
  if (image_ctx.old_format) {
    release_snap_id();
    return;
  } else if (cls::rbd::get_snap_namespace_type(m_snap_namespace) ==
               cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH) {
    get_snap();
    return;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::snapshot_trash_add(&op, m_snap_id);

  auto aio_comp = create_rados_callback<
    SnapshotRemoveRequest<I>,
    &SnapshotRemoveRequest<I>::handle_trash_snap>(this);
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void SnapshotRemoveRequest<I>::handle_trash_snap(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r == -EOPNOTSUPP) {
    // trash / clone v2 not supported
    detach_child();
    return;
  } else if (r < 0 && r != -EEXIST) {
    lderr(cct) << "failed to move snapshot to trash: " << cpp_strerror(r)
               << dendl;
    this->complete(r);
    return;
  }

  m_trashed_snapshot = true;
  get_snap();
}

template <typename I>
void SnapshotRemoveRequest<I>::get_snap() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  librados::ObjectReadOperation op;
  cls_client::snapshot_get_start(&op, {m_snap_id});

  auto aio_comp = create_rados_callback<
    SnapshotRemoveRequest<I>,
    &SnapshotRemoveRequest<I>::handle_get_snap>(this);
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, aio_comp, &op,
                                       &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void SnapshotRemoveRequest<I>::handle_get_snap(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r == 0) {
    std::vector<cls::rbd::SnapshotInfo> snap_infos;
    std::vector<ParentInfo> parents;
    std::vector<uint8_t> protections;
    bufferlist::iterator it = m_out_bl.begin();
    r = cls_client::snapshot_get_finish(&it, {m_snap_id}, &snap_infos,
                                        &parents, &protections);
    if (r == 0) {
      m_child_attached = (snap_infos[0].child_count > 0);
    }
  }

  if (r < 0) {
    lderr(cct) << "failed to retrieve snapshot: " << cpp_strerror(r)
               << dendl;
    this->complete(r);
    return;
  }

  detach_child();
}

template <typename I>
void SnapshotRemoveRequest<I>::detach_child() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  bool detach_child = false;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    RWLock::RLocker parent_locker(image_ctx.parent_lock);

    ParentSpec our_pspec;
    int r = image_ctx.get_parent_spec(m_snap_id, &our_pspec);
    if (r < 0) {
      if (r == -ENOENT) {
        ldout(cct, 1) << "No such snapshot" << dendl;
      } else {
        lderr(cct) << "failed to retrieve parent spec" << dendl;
      }

      this->async_complete(r);
      return;
    }

    if (image_ctx.parent_md.spec != our_pspec &&
        (scan_for_parents(our_pspec) == -ENOENT)) {
      // no other references to the parent image
      detach_child = true;
    }
  }

  if (!detach_child) {
    // HEAD image or other snapshots still associated with parent
    remove_object_map();
    return;
  }

  ldout(cct, 5) << dendl;
  auto ctx = create_context_callback<
    SnapshotRemoveRequest<I>,
    &SnapshotRemoveRequest<I>::handle_detach_child>(this);
  auto req = image::DetachChildRequest<I>::create(image_ctx, ctx);
  req->send();
}

template <typename I>
void SnapshotRemoveRequest<I>::handle_detach_child(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to detach child from parent: " << cpp_strerror(r)
               << dendl;
    this->complete(r);
    return;
  }

  remove_object_map();
}

template <typename I>
void SnapshotRemoveRequest<I>::remove_object_map() {
  I &image_ctx = this->m_image_ctx;
  if (m_child_attached) {
    // if a clone v2 child is attached to this snapshot, we cannot
    // proceed. It's only an error if the snap was already in the trash
    this->complete(m_trashed_snapshot ? 0 : -EBUSY);
    return;
  }

  CephContext *cct = image_ctx.cct;

  {
    RWLock::RLocker owner_lock(image_ctx.owner_lock);
    RWLock::WLocker snap_locker(image_ctx.snap_lock);
    RWLock::RLocker object_map_locker(image_ctx.object_map_lock);
    if (image_ctx.object_map != nullptr) {
      ldout(cct, 5) << dendl;

      auto ctx = create_context_callback<
        SnapshotRemoveRequest<I>,
        &SnapshotRemoveRequest<I>::handle_remove_object_map>(this);
      image_ctx.object_map->snapshot_remove(m_snap_id, ctx);
      return;
    }
  }

  // object map disabled
  release_snap_id();
}

template <typename I>
void SnapshotRemoveRequest<I>::handle_remove_object_map(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to remove snapshot object map: " << cpp_strerror(r)
               << dendl;
    this->complete(r);
    return;
  }

  release_snap_id();
}

template <typename I>
void SnapshotRemoveRequest<I>::release_snap_id() {
  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "snap_name=" << m_snap_name << ", "
                << "snap_id=" << m_snap_id << dendl;


  auto aio_comp = create_rados_callback<
    SnapshotRemoveRequest<I>,
    &SnapshotRemoveRequest<I>::handle_release_snap_id>(this);
  image_ctx.data_ctx.aio_selfmanaged_snap_remove(m_snap_id, aio_comp);
  aio_comp->release();
}

template <typename I>
void SnapshotRemoveRequest<I>::handle_release_snap_id(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to release snap id: " << cpp_strerror(r) << dendl;
    this->complete(r);
    return;
  }

  remove_snap();
}

template <typename I>
void SnapshotRemoveRequest<I>::remove_snap() {
  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  librados::ObjectWriteOperation op;
  if (image_ctx.old_format) {
    cls_client::old_snapshot_remove(&op, m_snap_name);
  } else {
    cls_client::snapshot_remove(&op, m_snap_id);
  }

  auto aio_comp = create_rados_callback<
    SnapshotRemoveRequest<I>,
    &SnapshotRemoveRequest<I>::handle_remove_snap>(this);
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void SnapshotRemoveRequest<I>::handle_remove_snap(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to remove snapshot: " << cpp_strerror(r) << dendl;
    this->complete(r);
    return;
  }

  remove_snap_context();
  this->complete(0);
}

template <typename I>
void SnapshotRemoveRequest<I>::remove_snap_context() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  RWLock::WLocker snap_locker(image_ctx.snap_lock);
  image_ctx.rm_snap(m_snap_namespace, m_snap_name, m_snap_id);
}

template <typename I>
int SnapshotRemoveRequest<I>::scan_for_parents(ParentSpec &pspec) {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.snap_lock.is_locked());
  assert(image_ctx.parent_lock.is_locked());

  if (pspec.pool_id != -1) {
    map<uint64_t, SnapInfo>::iterator it;
    for (it = image_ctx.snap_info.begin();
         it != image_ctx.snap_info.end(); ++it) {
      // skip our snap id (if checking base image, CEPH_NOSNAP won't match)
      if (it->first == m_snap_id) {
        continue;
      }
      if (it->second.parent.spec == pspec) {
        break;
      }
    }
    if (it == image_ctx.snap_info.end()) {
      return -ENOENT;
    }
  }
  return 0;
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotRemoveRequest<librbd::ImageCtx>;
