// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageSync.h"
#include "InstanceWatcher.h"
#include "ProgressContext.h"
#include "common/debug.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "librbd/DeepCopyRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/internal.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/deep_copy/Handler.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.h"
#include "tools/rbd_mirror/image_sync/SyncPointPruneRequest.h"
#include "tools/rbd_mirror/image_sync/Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageSync: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {

using namespace image_sync;
using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
class ImageSync<I>::ImageCopyProgressHandler
  : public librbd::deep_copy::NoOpHandler {
public:
  ImageCopyProgressHandler(ImageSync *image_sync) : image_sync(image_sync) {
  }

  int update_progress(uint64_t object_no, uint64_t object_count) override {
    image_sync->handle_copy_image_update_progress(object_no, object_count);
    return 0;
  }

  ImageSync *image_sync;
};

template <typename I>
ImageSync<I>::ImageSync(
    Threads<I>* threads,
    I *local_image_ctx,
    I *remote_image_ctx,
    const std::string &local_mirror_uuid,
    image_sync::SyncPointHandler* sync_point_handler,
    InstanceWatcher<I> *instance_watcher,
    ProgressContext *progress_ctx,
    Context *on_finish)
  : CancelableRequest("rbd::mirror::ImageSync", local_image_ctx->cct,
                      on_finish),
    m_threads(threads),
    m_local_image_ctx(local_image_ctx),
    m_remote_image_ctx(remote_image_ctx),
    m_local_mirror_uuid(local_mirror_uuid),
    m_sync_point_handler(sync_point_handler),
    m_instance_watcher(instance_watcher),
    m_progress_ctx(progress_ctx),
    m_lock(ceph::make_mutex(unique_lock_name("ImageSync::m_lock", this))),
    m_update_sync_point_interval(
      m_local_image_ctx->cct->_conf.template get_val<double>(
        "rbd_mirror_sync_point_update_age")) {
}

template <typename I>
ImageSync<I>::~ImageSync() {
  ceph_assert(m_image_copy_request == nullptr);
  ceph_assert(m_image_copy_prog_handler == nullptr);
  ceph_assert(m_update_sync_ctx == nullptr);
}

template <typename I>
void ImageSync<I>::send() {
  send_notify_sync_request();
}

template <typename I>
void ImageSync<I>::cancel() {
  std::lock_guard locker{m_lock};

  dout(10) << dendl;

  m_canceled = true;

  if (m_instance_watcher->cancel_sync_request(m_local_image_ctx->id)) {
    return;
  }

  if (m_image_copy_request != nullptr) {
    m_image_copy_request->cancel();
  }
}

template <typename I>
void ImageSync<I>::send_notify_sync_request() {
  update_progress("NOTIFY_SYNC_REQUEST");

  dout(10) << dendl;

  m_lock.lock();
  if (m_canceled) {
    m_lock.unlock();
    CancelableRequest::finish(-ECANCELED);
    return;
  }

  Context *ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
      ImageSync<I>, &ImageSync<I>::handle_notify_sync_request>(this));
  m_instance_watcher->notify_sync_request(m_local_image_ctx->id, ctx);
  m_lock.unlock();
}

template <typename I>
void ImageSync<I>::handle_notify_sync_request(int r) {
  dout(10) << ": r=" << r << dendl;

  m_lock.lock();
  if (r == 0 && m_canceled) {
    r = -ECANCELED;
  }
  m_lock.unlock();

  if (r < 0) {
    CancelableRequest::finish(r);
    return;
  }

  send_prune_catch_up_sync_point();
}

template <typename I>
void ImageSync<I>::send_prune_catch_up_sync_point() {
  update_progress("PRUNE_CATCH_UP_SYNC_POINT");

  if (m_sync_point_handler->get_sync_points().empty()) {
    send_create_sync_point();
    return;
  }

  dout(10) << dendl;

  // prune will remove sync points with missing snapshots and
  // ensure we have a maximum of one sync point (in case we
  // restarted)
  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_prune_catch_up_sync_point>(this);
  SyncPointPruneRequest<I> *request = SyncPointPruneRequest<I>::create(
    m_remote_image_ctx, false, m_sync_point_handler, ctx);
  request->send();
}

template <typename I>
void ImageSync<I>::handle_prune_catch_up_sync_point(int r) {
  dout(10) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to prune catch-up sync point: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_create_sync_point();
}

template <typename I>
void ImageSync<I>::send_create_sync_point() {
  update_progress("CREATE_SYNC_POINT");

  // TODO: when support for disconnecting laggy clients is added,
  //       re-connect and create catch-up sync point
  if (!m_sync_point_handler->get_sync_points().empty()) {
    send_copy_image();
    return;
  }

  dout(10) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_create_sync_point>(this);
  SyncPointCreateRequest<I> *request = SyncPointCreateRequest<I>::create(
    m_remote_image_ctx, m_local_mirror_uuid, m_sync_point_handler, ctx);
  request->send();
}

template <typename I>
void ImageSync<I>::handle_create_sync_point(int r) {
  dout(10) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create sync point: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  send_copy_image();
}

template <typename I>
void ImageSync<I>::send_copy_image() {
  librados::snap_t snap_id_start = 0;
  librados::snap_t snap_id_end;
  librbd::deep_copy::ObjectNumber object_number;
  int r = 0;

  m_snap_seqs_copy = m_sync_point_handler->get_snap_seqs();
  m_sync_points_copy = m_sync_point_handler->get_sync_points();
  ceph_assert(!m_sync_points_copy.empty());
  auto &sync_point = m_sync_points_copy.front();

  {
    std::shared_lock image_locker{m_remote_image_ctx->image_lock};
    snap_id_end = m_remote_image_ctx->get_snap_id(
	cls::rbd::UserSnapshotNamespace(), sync_point.snap_name);
    if (snap_id_end == CEPH_NOSNAP) {
      derr << ": failed to locate snapshot: " << sync_point.snap_name << dendl;
      r = -ENOENT;
    } else if (!sync_point.from_snap_name.empty()) {
      snap_id_start = m_remote_image_ctx->get_snap_id(
        cls::rbd::UserSnapshotNamespace(), sync_point.from_snap_name);
      if (snap_id_start == CEPH_NOSNAP) {
        derr << ": failed to locate from snapshot: "
             << sync_point.from_snap_name << dendl;
        r = -ENOENT;
      }
    }
    object_number = sync_point.object_number;
  }
  if (r < 0) {
    finish(r);
    return;
  }

  m_lock.lock();
  if (m_canceled) {
    m_lock.unlock();
    finish(-ECANCELED);
    return;
  }

  dout(10) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_copy_image>(this);
  m_image_copy_prog_handler = new ImageCopyProgressHandler(this);
  m_image_copy_request = librbd::DeepCopyRequest<I>::create(
      m_remote_image_ctx, m_local_image_ctx, snap_id_start, snap_id_end,
      0, false, object_number, m_threads->work_queue, &m_snap_seqs_copy,
      m_image_copy_prog_handler, ctx);
  m_image_copy_request->get();
  m_lock.unlock();

  update_progress("COPY_IMAGE");

  m_image_copy_request->send();
}

template <typename I>
void ImageSync<I>::handle_copy_image(int r) {
  dout(10) << ": r=" << r << dendl;

  {
    std::scoped_lock locker{m_threads->timer_lock, m_lock};
    m_image_copy_request->put();
    m_image_copy_request = nullptr;
    delete m_image_copy_prog_handler;
    m_image_copy_prog_handler = nullptr;
    if (r == 0 && m_canceled) {
      r = -ECANCELED;
    }

    if (m_update_sync_ctx != nullptr) {
      m_threads->timer->cancel_event(m_update_sync_ctx);
      m_update_sync_ctx = nullptr;
    }

    if (m_updating_sync_point) {
      m_ret_val = r;
      return;
    }
  }

  if (r == -ECANCELED) {
    dout(10) << ": image copy canceled" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << ": failed to copy image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_flush_sync_point();
}

template <typename I>
void ImageSync<I>::handle_copy_image_update_progress(uint64_t object_no,
                                                     uint64_t object_count) {
  int percent = 100 * object_no / object_count;
  update_progress("COPY_IMAGE " + stringify(percent) + "%");

  std::lock_guard locker{m_lock};
  m_image_copy_object_no = object_no;
  m_image_copy_object_count = object_count;

  if (m_update_sync_ctx == nullptr && !m_updating_sync_point) {
    send_update_sync_point();
  }
}

template <typename I>
void ImageSync<I>::send_update_sync_point() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  m_update_sync_ctx = nullptr;

  if (m_canceled) {
    return;
  }

  ceph_assert(!m_sync_points_copy.empty());
  auto sync_point = &m_sync_points_copy.front();

  if (sync_point->object_number &&
      (m_image_copy_object_no - 1) == sync_point->object_number.get()) {
    // update sync point did not progress since last sync
    return;
  }

  m_updating_sync_point = true;

  if (m_image_copy_object_no > 0) {
    sync_point->object_number = m_image_copy_object_no - 1;
  }

  auto ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_update_sync_point>(this);
  m_sync_point_handler->update_sync_points(m_snap_seqs_copy,
                                           m_sync_points_copy, false, ctx);
}

template <typename I>
void ImageSync<I>::handle_update_sync_point(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  {
    std::scoped_lock locker{m_threads->timer_lock, m_lock};
    m_updating_sync_point = false;

    if (m_image_copy_request != nullptr) {
      m_update_sync_ctx = new LambdaContext(
        [this](int r) {
	  std::lock_guard locker{m_lock};
          this->send_update_sync_point();
        });
      m_threads->timer->add_event_after(
        m_update_sync_point_interval, m_update_sync_ctx);
      return;
    }
  }

  send_flush_sync_point();
}

template <typename I>
void ImageSync<I>::send_flush_sync_point() {
  if (m_ret_val < 0) {
    finish(m_ret_val);
    return;
  }

  update_progress("FLUSH_SYNC_POINT");

  ceph_assert(!m_sync_points_copy.empty());
  auto sync_point = &m_sync_points_copy.front();

  if (m_image_copy_object_no > 0) {
    sync_point->object_number = m_image_copy_object_no - 1;
  } else {
    sync_point->object_number = boost::none;
  }

  auto ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_flush_sync_point>(this);
  m_sync_point_handler->update_sync_points(m_snap_seqs_copy,
                                           m_sync_points_copy, false, ctx);
}

template <typename I>
void ImageSync<I>::handle_flush_sync_point(int r) {
  dout(10) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update client data: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  send_prune_sync_points();
}

template <typename I>
void ImageSync<I>::send_prune_sync_points() {
  dout(10) << dendl;

  update_progress("PRUNE_SYNC_POINTS");

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_prune_sync_points>(this);
  SyncPointPruneRequest<I> *request = SyncPointPruneRequest<I>::create(
    m_remote_image_ctx, true, m_sync_point_handler, ctx);
  request->send();
}

template <typename I>
void ImageSync<I>::handle_prune_sync_points(int r) {
  dout(10) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to prune sync point: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_sync_point_handler->get_sync_points().empty()) {
    send_copy_image();
    return;
  }

  finish(0);
}

template <typename I>
void ImageSync<I>::update_progress(const std::string &description) {
  dout(20) << ": " << description << dendl;

  if (m_progress_ctx) {
    m_progress_ctx->update_progress("IMAGE_SYNC/" + description);
  }
}

template <typename I>
void ImageSync<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_instance_watcher->notify_sync_complete(m_local_image_ctx->id);
  CancelableRequest::finish(r);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageSync<librbd::ImageCtx>;
