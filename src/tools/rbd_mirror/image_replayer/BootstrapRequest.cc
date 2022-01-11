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
#include "tools/rbd_mirror/PoolMetaCache.h"
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
    Peers peers,
    InstanceWatcher<I>* instance_watcher,
    const std::string& global_image_id,
    const std::string& local_mirror_uuid,
    ::journal::CacheManagerHandler* cache_manager_handler,
    PoolMetaCache<I>* pool_meta_cache,
    ProgressContext* progress_ctx,
    StateBuilder<I>** state_builder,
    bool* do_resync,
    Context* on_finish)
  : CancelableRequest("rbd::mirror::image_replayer::BootstrapRequest",
		      reinterpret_cast<CephContext*>(local_io_ctx.cct()),
                      on_finish),
    m_threads(threads),
    m_local_io_ctx(local_io_ctx),
    m_peers(peers),
    m_instance_watcher(instance_watcher),
    m_global_image_id(global_image_id),
    m_local_mirror_uuid(local_mirror_uuid),
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

  auto state_builder = *m_state_builder;
  ceph_assert(r < 0 || state_builder != nullptr);
  if (r == -ENOENT) {
    dout(10) << "local image does not exist" << dendl;
  } else if (r < 0) {
    derr << "error preparing local image for replay: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  // image replayer will detect the name change (if any) at next
  // status update
  if (r >= 0 && !m_prepare_local_image_name.empty()) {
    std::unique_lock locker{m_lock};
    dout(10) << "set image to " << m_prepare_local_image_name << dendl;
    m_local_image_name = m_prepare_local_image_name;
  }

  if (state_builder != nullptr && state_builder->is_local_primary()) {
    dout(5) << "local image is primary" << dendl;
    finish(-ENOMSG);
    return;
  }

  m_it_peer = m_peers.begin();
  prepare_remote_image();
}

template <typename I>
void BootstrapRequest<I>::prepare_remote_image() {
  dout(10) << dendl;
  update_progress("PREPARE_REMOTE_IMAGE");

  if (m_it_peer == m_peers.end()) {
    dout(10) << "no peer left" << dendl;
    auto state_builder = *m_state_builder;

    if (state_builder == nullptr) {
      finish(-ENOENT);
      return;
    }
    if (state_builder->remote_image_id.empty() &&
        (state_builder->local_image_id.empty() || state_builder->is_linked())) {
      // There is no primary image and the local image doesn't exist or is
      // linked to a missing remote image
      finish(-ENOLINK);
      return;
    }

    finish(-ENOENT);
    return;
  }

  RemotePoolMeta remote_pool_meta;
  ceph_assert(m_pool_meta_cache->get_remote_pool_meta(
    m_it_peer->io_ctx.get_id(), m_it_peer->uuid, &remote_pool_meta) == 0);

  Context *ctx = create_context_callback<
    BootstrapRequest, &BootstrapRequest<I>::handle_prepare_remote_image>(this);
  auto req = image_replayer::PrepareRemoteImageRequest<I>::create(
    m_threads, m_local_io_ctx, m_it_peer->io_ctx, m_global_image_id,
    m_local_mirror_uuid, remote_pool_meta, m_cache_manager_handler,
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
    dout(5) << "local image is primary: peer=" << *m_it_peer << dendl;

    finish(-ENOMSG);
    return;
  } else if (r == -ENOENT || state_builder == nullptr) {
    dout(10) << "remote image does not exist";
    if (state_builder != nullptr) {
      *_dout << ", local_image_id=" << state_builder->local_image_id  << ", "
             << "remote_image_id=" << state_builder->remote_image_id << ", "
             << "is_linked=" << state_builder->is_linked();
    }
    *_dout << dendl;

    ++m_it_peer;
    if (state_builder != nullptr && state_builder->is_linked()) {
      // If the image is linked we should stop the processing
      m_it_peer = m_peers.end();
    }

    prepare_remote_image();
    return;
  } else if (!state_builder->is_remote_primary()) {
    ceph_assert(!state_builder->remote_image_id.empty());

    if (state_builder->local_image_id.empty()) {
      dout(10) << "local image does not exist and remote image is not primary"
               << dendl;
      ++m_it_peer;
      prepare_remote_image();
      return;
    } else if (!state_builder->is_linked()) {
      dout(10) << "local image is unlinked and remote image is not primary"
               << dendl;
      ++m_it_peer;
      prepare_remote_image();
      return;
    }
  } else if (r < 0) {
    derr << "error preparing remote image for replay: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  state_builder->remote_image_peer = *m_it_peer;
  open_remote_image();
  return;
}

template <typename I>
void BootstrapRequest<I>::open_remote_image() {
  auto state_builder = *m_state_builder;
  ceph_assert(state_builder != nullptr);
  auto remote_image_id = state_builder->remote_image_id;
  dout(15) << "remote_image_id=" << remote_image_id << dendl;

  update_progress("OPEN_REMOTE_IMAGE");

  auto ctx = create_context_callback<
    BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_open_remote_image>(this);
  ceph_assert(state_builder != nullptr);
  OpenImageRequest<I> *request = OpenImageRequest<I>::create(
    state_builder->remote_image_peer.io_ctx, &state_builder->remote_image_ctx,
    remote_image_id, false, ctx);
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
    finish(r);
    return;
  } else if (r < 0) {
    derr << "failed to open local image: " << cpp_strerror(r) << dendl;
    finish(r);
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
    derr << "failed to prepare local replay: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (*m_do_resync) {
    dout(10) << "local image resync requested" << dendl;
    finish(m_ret_val);
    return;
  } else if ((*m_state_builder)->is_disconnected()) {
    dout(10) << "client flagged disconnected -- skipping bootstrap" << dendl;
    // The caller is expected to detect disconnect initializing remote journal.
    finish(0);
    return;
  } else if (m_syncing) {
    dout(10) << "local image still syncing to remote image" << dendl;
    image_sync();
    return;
  }

  finish(m_ret_val);
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
    finish(r);
    return;
  }

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::image_sync() {
  std::unique_lock locker{m_lock};
  if (m_canceled) {
    locker.unlock();

    dout(10) << "request canceled" << dendl;
    finish(-ECANCELED);
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
