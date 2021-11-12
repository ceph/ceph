// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "BootstrapRequest.h"
#include "CreateImageRequest.h"
#include "OpenImageRequest.h"
#include "OpenLocalImageRequest.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "journal/Journaler.h"
#include "journal/Settings.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/journal/Types.h"
#include "tools/rbd_mirror/BaseRequest.h"
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/ProgressContext.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/PrepareLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"
#include "tools/rbd_mirror/image_replayer/journal/SyncPointHandler.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::" \
                           << "BootstrapRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
BootstrapRequest<I>::BootstrapRequest(
    Threads<I>* threads,
    librados::IoCtx& local_io_ctx,
    librados::IoCtx& remote_io_ctx,
    InstanceWatcher<I>* instance_watcher,
    const std::string& global_image_id,
    const std::string& local_mirror_uuid,
    const RemotePoolMeta& remote_pool_meta,
    ::journal::CacheManagerHandler* cache_manager_handler,
    PoolMetaCache* pool_meta_cache,
    ProgressContext* progress_ctx,
    StateBuilder<I>** state_builder,
    bool* do_resync,
    Context* on_finish)
  : CancelableRequest("rbd::mirror::image_replayer::BootstrapRequest",
		      reinterpret_cast<CephContext*>(local_io_ctx.cct()),
                      on_finish),
    m_threads(threads),
    m_local_io_ctx(local_io_ctx),
    m_remote_io_ctx(remote_io_ctx),
    m_instance_watcher(instance_watcher),
    m_global_image_id(global_image_id),
    m_local_mirror_uuid(local_mirror_uuid),
    m_remote_pool_meta(remote_pool_meta),
    m_cache_manager_handler(cache_manager_handler),
    m_pool_meta_cache(pool_meta_cache),
    m_progress_ctx(progress_ctx),
    m_state_builder(state_builder),
    m_do_resync(do_resync),
    m_lock(ceph::make_mutex(unique_lock_name("BootstrapRequest::m_lock",
                                             this))) {
  dout(10) << dendl;
}

template <typename I>
bool BootstrapRequest<I>::is_syncing() const {
  std::lock_guard locker{m_lock};
  return (m_image_sync != nullptr);
}

template <typename I>
void BootstrapRequest<I>::send() {
  *m_do_resync = false;

  prepare_local_image();
}

template <typename I>
void BootstrapRequest<I>::cancel() {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};
  m_canceled = true;

  if (m_image_sync != nullptr) {
    m_image_sync->cancel();
  }
}

template <typename I>
std::string BootstrapRequest<I>::get_local_image_name() const {
  std::unique_lock locker{m_lock};
  return m_local_image_name;
}

template <typename I>
void BootstrapRequest<I>::prepare_local_image() {
  dout(10) << dendl;
  update_progress("PREPARE_LOCAL_IMAGE");

  {
    std::unique_lock locker{m_lock};
    m_local_image_name = m_global_image_id;
  }

  ceph_assert(*m_state_builder == nullptr);
  auto ctx = create_context_callback<
    BootstrapRequest, &BootstrapRequest<I>::handle_prepare_local_image>(this);
  auto req = image_replayer::PrepareLocalImageRequest<I>::create(
    m_local_io_ctx, m_global_image_id, &m_prepare_local_image_name,
    m_state_builder, m_threads->work_queue, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_prepare_local_image(int r) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(r < 0 || *m_state_builder != nullptr);
  if (r == -ENOENT) {
    dout(10) << "local image does not exist" << dendl;
  } else if (r < 0) {
    derr << "error preparing local image for replay" << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  // image replayer will detect the name change (if any) at next
  // status update
  if (r >= 0 && !m_prepare_local_image_name.empty()) {
    std::unique_lock locker{m_lock};
    m_local_image_name = m_prepare_local_image_name;
  }

  prepare_remote_image();
}

template <typename I>
void BootstrapRequest<I>::prepare_remote_image() {
  dout(10) << dendl;
  update_progress("PREPARE_REMOTE_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest, &BootstrapRequest<I>::handle_prepare_remote_image>(this);
  auto req = image_replayer::PrepareRemoteImageRequest<I>::create(
    m_threads, m_local_io_ctx, m_remote_io_ctx, m_global_image_id,
    m_local_mirror_uuid, m_remote_pool_meta, m_cache_manager_handler,
    m_state_builder, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_prepare_remote_image(int r) {
  dout(10) << "r=" << r << dendl;

  auto state_builder = *m_state_builder;
  ceph_assert(state_builder == nullptr ||
              !state_builder->remote_mirror_uuid.empty());

  if (state_builder != nullptr && state_builder->is_local_primary()) {
    dout(5) << "local image is primary" << dendl;
    finish(-ENOMSG);
    return;
  } else if (r == -EREMOTEIO) {
    dout(10) << "remote-image is non-primary" << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (r == -ENOENT || state_builder == nullptr) {
    dout(10) << "remote image does not exist";
    if (state_builder != nullptr) {
      *_dout << ": "
             << "local_image_id=" << state_builder->local_image_id  << ", "
             << "remote_image_id=" << state_builder->remote_image_id << ", "
             << "is_linked=" << state_builder->is_linked();
    }
    *_dout << dendl;

    // TODO need to support multiple remote images
    if (state_builder != nullptr &&
        state_builder->remote_image_id.empty() &&
        (state_builder->local_image_id.empty() ||
         state_builder->is_linked())) {
      // both images doesn't exist or local image exists and is non-primary
      // and linked to the missing remote image
      finish(-ENOLINK);
    } else {
      finish(-ENOENT);
    }
    return;
  } else if (r < 0) {
    derr << "error retrieving remote image id" << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  open_remote_image();
}

template <typename I>
void BootstrapRequest<I>::open_remote_image() {
  ceph_assert(*m_state_builder != nullptr);
  auto remote_image_id = (*m_state_builder)->remote_image_id;
  dout(15) << "remote_image_id=" << remote_image_id << dendl;

  update_progress("OPEN_REMOTE_IMAGE");

  auto ctx = create_context_callback<
    BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_open_remote_image>(this);
  ceph_assert(*m_state_builder != nullptr);
  OpenImageRequest<I> *request = OpenImageRequest<I>::create(
    m_remote_io_ctx, &(*m_state_builder)->remote_image_ctx, remote_image_id,
    false, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_remote_image(int r) {
  dout(15) << "r=" << r << dendl;

  ceph_assert(*m_state_builder != nullptr);
  if (r < 0) {
    derr << "failed to open remote image: " << cpp_strerror(r) << dendl;
    ceph_assert((*m_state_builder)->remote_image_ctx == nullptr);
    finish(r);
    return;
  }

  if ((*m_state_builder)->local_image_id.empty()) {
    create_local_image();
    return;
  }

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::open_local_image() {
  ceph_assert(*m_state_builder != nullptr);
  auto local_image_id = (*m_state_builder)->local_image_id;

  dout(15) << "local_image_id=" << local_image_id << dendl;

  update_progress("OPEN_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_local_image>(
      this);
  OpenLocalImageRequest<I> *request = OpenLocalImageRequest<I>::create(
    m_local_io_ctx, &(*m_state_builder)->local_image_ctx, local_image_id,
    m_threads->work_queue, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_local_image(int r) {
  dout(15) << "r=" << r << dendl;

  ceph_assert(*m_state_builder != nullptr);
  auto local_image_ctx = (*m_state_builder)->local_image_ctx;
  ceph_assert((r >= 0 && local_image_ctx != nullptr) ||
              (r < 0 && local_image_ctx == nullptr));

  if (r == -ENOENT) {
    dout(10) << "local image missing" << dendl;
    create_local_image();
    return;
  } else if (r == -EREMOTEIO) {
    dout(10) << "local image is primary -- skipping image replay" << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  } else if (r < 0) {
    derr << "failed to open local image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  prepare_replay();
}

template <typename I>
void BootstrapRequest<I>::prepare_replay() {
  dout(10) << dendl;
  update_progress("PREPARE_REPLAY");

  ceph_assert(*m_state_builder != nullptr);
  auto ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_prepare_replay>(this);
  auto request = (*m_state_builder)->create_prepare_replay_request(
    m_local_mirror_uuid, m_progress_ctx, m_do_resync, &m_syncing, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_prepare_replay(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    if (r != -EREMOTEIO) {
      derr << "failed to prepare local replay: " << cpp_strerror(r) << dendl;
    }
    m_ret_val = r;
    close_remote_image();
    return;
  } else if (*m_do_resync) {
    dout(10) << "local image resync requested" << dendl;
    close_remote_image();
    return;
  } else if ((*m_state_builder)->is_disconnected()) {
    dout(10) << "client flagged disconnected -- skipping bootstrap" << dendl;
    // The caller is expected to detect disconnect initializing remote journal.
    m_ret_val = 0;
    close_remote_image();
    return;
  } else if (m_syncing) {
    dout(10) << "local image still syncing to remote image" << dendl;
    image_sync();
    return;
  }

  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::create_local_image() {
  dout(10) << dendl;
  update_progress("CREATE_LOCAL_IMAGE");

  ceph_assert(*m_state_builder != nullptr);
  auto ctx = create_context_callback<
    BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_create_local_image>(this);
  auto request = (*m_state_builder)->create_local_image_request(
    m_threads, m_local_io_ctx, m_global_image_id, m_pool_meta_cache,
    m_progress_ctx, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_image(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    if (r == -ENOENT) {
      dout(10) << "parent image does not exist" << dendl;
    } else {
      derr << "failed to create local image: " << cpp_strerror(r) << dendl;
    }
    m_ret_val = r;
    close_remote_image();
    return;
  }

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::image_sync() {
  std::unique_lock locker{m_lock};
  if (m_canceled) {
    locker.unlock();

    m_ret_val = -ECANCELED;
    dout(10) << "request canceled" << dendl;
    close_remote_image();
    return;
  }

  dout(15) << dendl;
  ceph_assert(m_image_sync == nullptr);

  auto state_builder = *m_state_builder;
  auto sync_point_handler = state_builder->create_sync_point_handler();

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_image_sync>(this);
  m_image_sync = ImageSync<I>::create(
    m_threads, state_builder->local_image_ctx, state_builder->remote_image_ctx,
    m_local_mirror_uuid, sync_point_handler, m_instance_watcher,
    m_progress_ctx, ctx);
  m_image_sync->get();
  locker.unlock();

  update_progress("IMAGE_SYNC");
  m_image_sync->send();
}

template <typename I>
void BootstrapRequest<I>::handle_image_sync(int r) {
  dout(15) << "r=" << r << dendl;

  {
    std::lock_guard locker{m_lock};
    m_image_sync->put();
    m_image_sync = nullptr;

    (*m_state_builder)->destroy_sync_point_handler();
  }

  if (r < 0) {
    if (r == -ECANCELED) {
      dout(10) << "request canceled" << dendl;
    } else {
      derr << "failed to sync remote image: " << cpp_strerror(r) << dendl;
    }
    m_ret_val = r;
  }

  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::close_remote_image() {
  if ((*m_state_builder)->replay_requires_remote_image()) {
    finish(m_ret_val);
    return;
  }

  dout(15) << dendl;

  update_progress("CLOSE_REMOTE_IMAGE");

  auto ctx = create_context_callback<
    BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_close_remote_image>(this);
  ceph_assert(*m_state_builder != nullptr);
  (*m_state_builder)->close_remote_image(ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_close_remote_image(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error encountered closing remote image: " << cpp_strerror(r)
         << dendl;
  }

  finish(m_ret_val);
}

template <typename I>
void BootstrapRequest<I>::update_progress(const std::string &description) {
  dout(15) << description << dendl;

  if (m_progress_ctx) {
    m_progress_ctx->update_progress(description);
  }
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::BootstrapRequest<librbd::ImageCtx>;
