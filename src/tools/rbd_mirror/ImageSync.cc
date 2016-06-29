// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageSync.h"
#include "ProgressContext.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/journal/Types.h"
#include "tools/rbd_mirror/image_sync/ImageCopyRequest.h"
#include "tools/rbd_mirror/image_sync/SnapshotCopyRequest.h"
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.h"
#include "tools/rbd_mirror/image_sync/SyncPointPruneRequest.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageSync: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {

using namespace image_sync;
using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
ImageSync<I>::ImageSync(I *local_image_ctx, I *remote_image_ctx,
                        SafeTimer *timer, Mutex *timer_lock,
                        const std::string &mirror_uuid, Journaler *journaler,
                        MirrorPeerClientMeta *client_meta,
                        ContextWQ *work_queue, Context *on_finish,
			ProgressContext *progress_ctx)
  : BaseRequest("rbd::mirror::ImageSync", local_image_ctx->cct, on_finish),
    m_local_image_ctx(local_image_ctx), m_remote_image_ctx(remote_image_ctx),
    m_timer(timer), m_timer_lock(timer_lock), m_mirror_uuid(mirror_uuid),
    m_journaler(journaler), m_client_meta(client_meta),
    m_work_queue(work_queue), m_progress_ctx(progress_ctx),
    m_lock(unique_lock_name("ImageSync::m_lock", this)) {
}

template <typename I>
ImageSync<I>::~ImageSync() {
  assert(m_snapshot_copy_request == nullptr);
  assert(m_image_copy_request == nullptr);
}

template <typename I>
void ImageSync<I>::send() {
  send_prune_catch_up_sync_point();
}

template <typename I>
void ImageSync<I>::cancel() {
  Mutex::Locker locker(m_lock);

  dout(20) << dendl;

  m_canceled = true;

  if (m_snapshot_copy_request != nullptr) {
    m_snapshot_copy_request->cancel();
  }

  if (m_image_copy_request != nullptr) {
    m_image_copy_request->cancel();
  }
}

template <typename I>
void ImageSync<I>::send_prune_catch_up_sync_point() {
  update_progress("PRUNE_CATCH_UP_SYNC_POINT");

  if (m_client_meta->sync_points.empty()) {
    send_create_sync_point();
    return;
  }

  dout(20) << dendl;

  // prune will remove sync points with missing snapshots and
  // ensure we have a maximum of one sync point (in case we
  // restarted)
  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_prune_catch_up_sync_point>(this);
  SyncPointPruneRequest<I> *request = SyncPointPruneRequest<I>::create(
    m_remote_image_ctx, false, m_journaler, m_client_meta, ctx);
  request->send();
}

template <typename I>
void ImageSync<I>::handle_prune_catch_up_sync_point(int r) {
  dout(20) << ": r=" << r << dendl;

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
  if (m_client_meta->sync_points.size() > 0) {
    send_copy_snapshots();
    return;
  }

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_create_sync_point>(this);
  SyncPointCreateRequest<I> *request = SyncPointCreateRequest<I>::create(
    m_remote_image_ctx, m_mirror_uuid, m_journaler, m_client_meta, ctx);
  request->send();
}

template <typename I>
void ImageSync<I>::handle_create_sync_point(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create sync point: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  send_copy_snapshots();
}

template <typename I>
void ImageSync<I>::send_copy_snapshots() {
  m_lock.Lock();
  if (m_canceled) {
    m_lock.Unlock();
    finish(-ECANCELED);
    return;
  }

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_copy_snapshots>(this);
  m_snapshot_copy_request = SnapshotCopyRequest<I>::create(
    m_local_image_ctx, m_remote_image_ctx, &m_snap_map, m_journaler,
    m_client_meta, m_work_queue, ctx);
  m_snapshot_copy_request->get();
  m_lock.Unlock();

  update_progress("COPY_SNAPSHOTS");

  m_snapshot_copy_request->send();
}

template <typename I>
void ImageSync<I>::handle_copy_snapshots(int r) {
  dout(20) << ": r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    m_snapshot_copy_request->put();
    m_snapshot_copy_request = nullptr;
    if (r == 0 && m_canceled) {
      r = -ECANCELED;
    }
  }

  if (r == -ECANCELED) {
    dout(10) << ": snapshot copy canceled" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << ": failed to copy snapshot metadata: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_copy_image();
}

template <typename I>
void ImageSync<I>::send_copy_image() {
  m_lock.Lock();
  if (m_canceled) {
    m_lock.Unlock();
    finish(-ECANCELED);
    return;
  }

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_copy_image>(this);
  m_image_copy_request = ImageCopyRequest<I>::create(
    m_local_image_ctx, m_remote_image_ctx, m_timer, m_timer_lock,
    m_journaler, m_client_meta, &m_client_meta->sync_points.front(),
    ctx, m_progress_ctx);
  m_image_copy_request->get();
  m_lock.Unlock();

  update_progress("COPY_IMAGE");

  m_image_copy_request->send();
}

template <typename I>
void ImageSync<I>::handle_copy_image(int r) {
  dout(20) << ": r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    m_image_copy_request->put();
    m_image_copy_request = nullptr;
    if (r == 0 && m_canceled) {
      r = -ECANCELED;
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

  send_copy_object_map();
}

template <typename I>
void ImageSync<I>::send_copy_object_map() {
  update_progress("COPY_OBJECT_MAP");

  m_local_image_ctx->snap_lock.get_read();
  if (!m_local_image_ctx->test_features(RBD_FEATURE_OBJECT_MAP,
                                        m_local_image_ctx->snap_lock)) {
    m_local_image_ctx->snap_lock.put_read();
    send_prune_sync_points();
    return;
  }

  assert(m_local_image_ctx->object_map != nullptr);

  assert(!m_client_meta->sync_points.empty());
  librbd::journal::MirrorPeerSyncPoint &sync_point =
    m_client_meta->sync_points.front();
  auto snap_id_it = m_local_image_ctx->snap_ids.find(sync_point.snap_name);
  assert(snap_id_it != m_local_image_ctx->snap_ids.end());
  librados::snap_t snap_id = snap_id_it->second;

  dout(20) << ": snap_id=" << snap_id << ", "
           << "snap_name=" << sync_point.snap_name << dendl;

  // rollback the object map (copy snapshot object map to HEAD)
  RWLock::WLocker object_map_locker(m_local_image_ctx->object_map_lock);
  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_copy_object_map>(this);
  m_local_image_ctx->object_map->rollback(snap_id, ctx);
  m_local_image_ctx->snap_lock.put_read();
}

template <typename I>
void ImageSync<I>::handle_copy_object_map(int r) {
  dout(20) << dendl;

  assert(r == 0);
  send_refresh_object_map();
}

template <typename I>
void ImageSync<I>::send_refresh_object_map() {
  dout(20) << dendl;

  update_progress("REFRESH_OBJECT_MAP");

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_refresh_object_map>(this);
  m_object_map = m_local_image_ctx->create_object_map(CEPH_NOSNAP);
  m_object_map->open(ctx);
}

template <typename I>
void ImageSync<I>::handle_refresh_object_map(int r) {
  dout(20) << dendl;

  assert(r == 0);
  {
    RWLock::WLocker snap_locker(m_local_image_ctx->snap_lock);
    std::swap(m_local_image_ctx->object_map, m_object_map);
  }
  delete m_object_map;

  send_prune_sync_points();
}

template <typename I>
void ImageSync<I>::send_prune_sync_points() {
  dout(20) << dendl;

  update_progress("PRUNE_SYNC_POINTS");

  Context *ctx = create_context_callback<
    ImageSync<I>, &ImageSync<I>::handle_prune_sync_points>(this);
  SyncPointPruneRequest<I> *request = SyncPointPruneRequest<I>::create(
    m_remote_image_ctx, true, m_journaler, m_client_meta, ctx);
  request->send();
}

template <typename I>
void ImageSync<I>::handle_prune_sync_points(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to prune sync point: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_client_meta->sync_points.empty()) {
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

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageSync<librbd::ImageCtx>;
