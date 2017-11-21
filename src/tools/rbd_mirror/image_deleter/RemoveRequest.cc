// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_deleter/RemoveRequest.h"
#include "include/assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/image/RemoveRequest.h"
#include "tools/rbd_mirror/image_deleter/SnapshotPurgeRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_deleter::RemoveRequest: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_deleter {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void RemoveRequest<I>::send() {
  *m_error_result = ERROR_RESULT_RETRY;

  get_mirror_image_id();
}

template <typename I>
void RemoveRequest<I>::get_mirror_image_id() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_image_id_start(&op, m_global_image_id);

  auto aio_comp = create_rados_callback<
    RemoveRequest<I>, &RemoveRequest<I>::handle_get_mirror_image_id>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveRequest<I>::handle_get_mirror_image_id(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == 0) {
    auto bl_it = m_out_bl.begin();
    r = librbd::cls_client::mirror_image_get_image_id_finish(&bl_it,
                                                             &m_image_id);
  }
  if (r == -ENOENT) {
    dout(10) << "image " << m_global_image_id << " is not mirrored" << dendl;
    *m_error_result = ERROR_RESULT_COMPLETE;
    finish(r);
    return;
  } else if (r < 0) {
    derr << "error retrieving local id for image " << m_global_image_id
         << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_tag_owner();
}

template <typename I>
void RemoveRequest<I>::get_tag_owner() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    RemoveRequest<I>, &RemoveRequest<I>::handle_get_tag_owner>(this);
  librbd::Journal<I>::get_tag_owner(m_io_ctx, m_image_id, &m_mirror_uuid,
                                    m_op_work_queue, ctx);
}

template <typename I>
void RemoveRequest<I>::handle_get_tag_owner(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error retrieving image primary info for image "
         << m_global_image_id << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (r != -ENOENT) {
    if (m_mirror_uuid == librbd::Journal<>::LOCAL_MIRROR_UUID) {
      dout(10) << "image " << m_global_image_id << " is local primary" << dendl;

      *m_error_result = ERROR_RESULT_COMPLETE;
      finish(-EPERM);
      return;
    } else if (m_mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID &&
               !m_ignore_orphaned) {
      dout(10) << "image " << m_global_image_id << " is orphaned" << dendl;

      *m_error_result = ERROR_RESULT_COMPLETE;
      finish(-EPERM);
      return;
    }
  }

  get_snap_context();
}

template <typename I>
void RemoveRequest<I>::get_snap_context() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::get_snapcontext_start(&op);

  std::string header_oid = librbd::util::header_name(m_image_id);

  auto aio_comp = create_rados_callback<
    RemoveRequest<I>, &RemoveRequest<I>::handle_get_snap_context>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(header_oid, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveRequest<I>::handle_get_snap_context(int r) {
  dout(10) << "r=" << r << dendl;

  ::SnapContext snapc;
  if (r == 0) {
    auto bl_it = m_out_bl.begin();
    r = librbd::cls_client::get_snapcontext_finish(&bl_it, &snapc);
  }
  if (r < 0 && r != -ENOENT) {
    derr << "error retrieving snapshot context for image "
         << m_global_image_id << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_has_snapshots = (!snapc.empty());
  set_mirror_image_disabling();
}

template <typename I>
void RemoveRequest<I>::set_mirror_image_disabling() {
  dout(10) << dendl;

  cls::rbd::MirrorImage mirror_image;
  mirror_image.global_image_id = m_global_image_id;
  mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_set(&op, m_image_id, mirror_image);

  auto aio_comp = create_rados_callback<
    RemoveRequest<I>,
    &RemoveRequest<I>::handle_set_mirror_image_disabling>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveRequest<I>::handle_set_mirror_image_disabling(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << "local image is not mirrored, aborting deletion." << dendl;
    *m_error_result = ERROR_RESULT_COMPLETE;
    finish(r);
    return;
  } else if (r == -EEXIST || r == -EINVAL) {
    derr << "cannot disable mirroring for image " << m_global_image_id
         << ": global_image_id has changed/reused: "
         << cpp_strerror(r) << dendl;
    *m_error_result = ERROR_RESULT_COMPLETE;
    finish(r);
    return;
  } else if (r < 0) {
    derr << "cannot disable mirroring for image " << m_global_image_id
         << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  purge_snapshots();
}

template <typename I>
void RemoveRequest<I>::purge_snapshots() {
  if (!m_has_snapshots) {
    remove_image();
    return;
  }

  dout(10) << dendl;
  auto ctx = create_context_callback<
    RemoveRequest<I>, &RemoveRequest<I>::handle_purge_snapshots>(this);
  auto req = SnapshotPurgeRequest<I>::create(m_io_ctx, m_image_id, ctx);
  req->send();
}

template <typename I>
void RemoveRequest<I>::handle_purge_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -EBUSY) {
    dout(10) << "snapshots still in-use" << dendl;
    *m_error_result = ERROR_RESULT_RETRY_IMMEDIATELY;
    finish(r);
    return;
  } else if (r < 0) {
    derr << "failed to purge image snapshots: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_image();
}

template <typename I>
void RemoveRequest<I>::remove_image() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    RemoveRequest<I>, &RemoveRequest<I>::handle_remove_image>(this);
  auto req = librbd::image::RemoveRequest<I>::create(
    m_io_ctx, "", m_image_id, true, false, m_progress_ctx, m_op_work_queue,
    ctx);
  req->send();
}

template <typename I>
void RemoveRequest<I>::handle_remove_image(int r) {
  dout(10) << "r=" << r << dendl;
  if (r < 0 && r != -ENOENT) {
    derr << "error removing image " << m_global_image_id << " "
         << "(" << m_image_id << ") from local pool: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_mirror_image();
}

template <typename I>
void RemoveRequest<I>::remove_mirror_image() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_remove(&op, m_image_id);

  auto aio_comp = create_rados_callback<
    RemoveRequest<I>, &RemoveRequest<I>::handle_remove_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveRequest<I>::handle_remove_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;
  if (r < 0 && r != -ENOENT) {
    derr << "error removing image " << m_global_image_id << " from mirroring "
         << "directory: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void RemoveRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_deleter::RemoveRequest<librbd::ImageCtx>;
