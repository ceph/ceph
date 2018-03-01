// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_deleter/RemoveRequest.h"
#include "include/assert.h"
#include "common/debug.h"
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
         << m_image_id << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_has_snapshots = (!snapc.empty());
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
    m_io_ctx, "", m_image_id, true, true, m_progress_ctx, m_op_work_queue,
    ctx);
  req->send();
}

template <typename I>
void RemoveRequest<I>::handle_remove_image(int r) {
  dout(10) << "r=" << r << dendl;
  if (r == -ENOTEMPTY) {
    // image must have clone v2 snapshot still associated to child
    dout(10) << "snapshots still in-use" << dendl;
    *m_error_result = ERROR_RESULT_RETRY_IMMEDIATELY;
    finish(-EBUSY);
    return;
  }

  if (r < 0 && r != -ENOENT) {
    derr << "error removing image " << m_image_id << " "
         << "(" << m_image_id << ") from local pool: "
         << cpp_strerror(r) << dendl;
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
