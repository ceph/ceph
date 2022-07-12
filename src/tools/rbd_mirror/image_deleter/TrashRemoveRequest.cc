// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_deleter/TrashRemoveRequest.h"
#include "include/ceph_assert.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/TrashWatcher.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/trash/RemoveRequest.h"
#include "tools/rbd_mirror/image_deleter/SnapshotPurgeRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_deleter::TrashRemoveRequest: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_deleter {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void TrashRemoveRequest<I>::send() {
  *m_error_result = ERROR_RESULT_RETRY;

  get_trash_image_spec();
}

template <typename I>
void TrashRemoveRequest<I>::get_trash_image_spec() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::trash_get_start(&op, m_image_id);

  auto aio_comp = create_rados_callback<
    TrashRemoveRequest<I>,
    &TrashRemoveRequest<I>::handle_get_trash_image_spec>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(RBD_TRASH, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void TrashRemoveRequest<I>::handle_get_trash_image_spec(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == 0) {
    auto bl_it = m_out_bl.cbegin();
    r = librbd::cls_client::trash_get_finish(&bl_it, &m_trash_image_spec);
  }

  if (r == -ENOENT || (r >= 0 && m_trash_image_spec.source !=
                                   cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING)) {
    dout(10) << "image id " << m_image_id << " not in mirroring trash" << dendl;
    finish(0);
    return;
  } else if (r < 0) {
    derr << "error getting image id " << m_image_id << " info from trash: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_trash_image_spec.state != cls::rbd::TRASH_IMAGE_STATE_NORMAL &&
      m_trash_image_spec.state != cls::rbd::TRASH_IMAGE_STATE_REMOVING) {
    dout(10) << "image " << m_image_id << " is not in an expected trash state: "
             << m_trash_image_spec.state << dendl;
    *m_error_result = ERROR_RESULT_RETRY_IMMEDIATELY;
    finish(-EBUSY);
    return;
  }

  set_trash_state();
}

template <typename I>
void TrashRemoveRequest<I>::set_trash_state() {
  if (m_trash_image_spec.state == cls::rbd::TRASH_IMAGE_STATE_REMOVING) {
    get_snap_context();
    return;
  }

  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::trash_state_set(&op, m_image_id,
                                      cls::rbd::TRASH_IMAGE_STATE_REMOVING,
                                      cls::rbd::TRASH_IMAGE_STATE_NORMAL);

  auto aio_comp = create_rados_callback<
    TrashRemoveRequest<I>,
    &TrashRemoveRequest<I>::handle_set_trash_state>(this);
  int r = m_io_ctx.aio_operate(RBD_TRASH, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void TrashRemoveRequest<I>::handle_set_trash_state(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << "image id " << m_image_id << " not in mirroring trash" << dendl;
    finish(0);
    return;
  } else if (r < 0 && r != -EOPNOTSUPP) {
    derr << "error setting trash image state for image id " << m_image_id
         << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_snap_context();
}

template <typename I>
void TrashRemoveRequest<I>::get_snap_context() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::get_snapcontext_start(&op);

  std::string header_oid = librbd::util::header_name(m_image_id);

  auto aio_comp = create_rados_callback<
    TrashRemoveRequest<I>,
    &TrashRemoveRequest<I>::handle_get_snap_context>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(header_oid, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void TrashRemoveRequest<I>::handle_get_snap_context(int r) {
  dout(10) << "r=" << r << dendl;

  ::SnapContext snapc;
  if (r == 0) {
    auto bl_it = m_out_bl.cbegin();
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
void TrashRemoveRequest<I>::purge_snapshots() {
  if (!m_has_snapshots) {
    remove_image();
    return;
  }

  dout(10) << dendl;
  auto ctx = create_context_callback<
    TrashRemoveRequest<I>,
    &TrashRemoveRequest<I>::handle_purge_snapshots>(this);
  auto req = SnapshotPurgeRequest<I>::create(m_io_ctx, m_image_id, ctx);
  req->send();
}

template <typename I>
void TrashRemoveRequest<I>::handle_purge_snapshots(int r) {
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
void TrashRemoveRequest<I>::remove_image() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    TrashRemoveRequest<I>,
    &TrashRemoveRequest<I>::handle_remove_image>(this);
  auto req = librbd::trash::RemoveRequest<I>::create(
    m_io_ctx, m_image_id, m_op_work_queue, true, m_progress_ctx,
    ctx);
  req->send();
}

template <typename I>
void TrashRemoveRequest<I>::handle_remove_image(int r) {
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

  notify_trash_removed();
}

template <typename I>
void TrashRemoveRequest<I>::notify_trash_removed() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    TrashRemoveRequest<I>,
    &TrashRemoveRequest<I>::handle_notify_trash_removed>(this);
  librbd::TrashWatcher<I>::notify_image_removed(m_io_ctx, m_image_id, ctx);
}

template <typename I>
void TrashRemoveRequest<I>::handle_notify_trash_removed(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to notify trash watchers: " << cpp_strerror(r) << dendl;
  }

  finish(0);
}

template <typename I>
void TrashRemoveRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_deleter::TrashRemoveRequest<librbd::ImageCtx>;
