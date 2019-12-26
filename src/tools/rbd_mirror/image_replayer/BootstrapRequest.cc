// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "BootstrapRequest.h"
#include "CloseImageRequest.h"
#include "CreateImageRequest.h"
#include "OpenImageRequest.h"
#include "OpenLocalImageRequest.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "journal/Journaler.h"
#include "journal/Settings.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/journal/Types.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "tools/rbd_mirror/ProgressContext.h"
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/PrepareLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/CreateLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/PrepareReplayRequest.h"

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
    const std::string& remote_image_id,
    const std::string& global_image_id,
    const std::string& local_mirror_uuid,
    ::journal::CacheManagerHandler* cache_manager_handler,
    ProgressContext* progress_ctx,
    I** local_image_ctx,
    std::string* local_image_id,
    std::string* remote_mirror_uuid,
    Journaler** remote_journaler,
    bool* do_resync,
    Context* on_finish)
  : BaseRequest("rbd::mirror::image_replayer::BootstrapRequest",
		reinterpret_cast<CephContext*>(local_io_ctx.cct()), on_finish),
    m_threads(threads),
    m_local_io_ctx(local_io_ctx),
    m_remote_io_ctx(remote_io_ctx),
    m_instance_watcher(instance_watcher),
    m_remote_image_id(remote_image_id),
    m_global_image_id(global_image_id),
    m_local_mirror_uuid(local_mirror_uuid),
    m_cache_manager_handler(cache_manager_handler),
    m_progress_ctx(progress_ctx),
    m_local_image_ctx(local_image_ctx),
    m_local_image_id(local_image_id),
    m_remote_mirror_uuid(remote_mirror_uuid),
    m_remote_journaler(remote_journaler),
    m_do_resync(do_resync),
    m_lock(ceph::make_mutex(unique_lock_name("BootstrapRequest::m_lock",
                                             this))) {
  dout(10) << dendl;
}

template <typename I>
BootstrapRequest<I>::~BootstrapRequest() {
  ceph_assert(m_remote_image_ctx == nullptr);
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

  *m_local_image_id = "";
  m_local_image_name = m_global_image_id;
  auto ctx = create_context_callback<
    BootstrapRequest, &BootstrapRequest<I>::handle_prepare_local_image>(this);
  auto req = image_replayer::PrepareLocalImageRequest<I>::create(
    m_local_io_ctx, m_global_image_id, m_local_image_id,
    &m_prepare_local_image_name, &m_local_image_tag_owner,
    m_threads->work_queue, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_prepare_local_image(int r) {
  dout(10) << "r=" << r << dendl;

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
  {
    std::unique_lock locker{m_lock};
    m_local_image_name = m_prepare_local_image_name;
  }

  prepare_remote_image();
}

template <typename I>
void BootstrapRequest<I>::prepare_remote_image() {
  dout(10) << dendl;
  update_progress("PREPARE_REMOTE_IMAGE");

  auto cct = static_cast<CephContext *>(m_local_io_ctx.cct());
  ::journal::Settings journal_settings;
  journal_settings.commit_interval = cct->_conf.get_val<double>(
    "rbd_mirror_journal_commit_age");

  ceph_assert(*m_remote_journaler == nullptr);

  Context *ctx = create_context_callback<
    BootstrapRequest, &BootstrapRequest<I>::handle_prepare_remote_image>(this);
  auto req = image_replayer::PrepareRemoteImageRequest<I>::create(
    m_threads, m_remote_io_ctx, m_global_image_id, m_local_mirror_uuid,
    *m_local_image_id, journal_settings, m_cache_manager_handler,
    m_remote_mirror_uuid, &m_remote_image_id, m_remote_journaler,
    &m_client_state, &m_client_meta, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_prepare_remote_image(int r) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(r < 0 ? *m_remote_journaler == nullptr :
                      *m_remote_journaler != nullptr);
  if (r < 0 && !m_local_image_id->empty() &&
      m_local_image_tag_owner == librbd::Journal<>::LOCAL_MIRROR_UUID) {
    // local image is primary -- fall-through
  } else if (r == -ENOENT) {
    dout(10) << "remote image does not exist" << dendl;

    // TODO need to support multiple remote images
    if (m_remote_image_id.empty() && !m_local_image_id->empty() &&
        m_local_image_tag_owner == *m_remote_mirror_uuid) {
      // local image exists and is non-primary and linked to the missing
      // remote image
      finish(-ENOLINK);
    } else {
      dout(10) << "remote image does not exist" << dendl;
      finish(-ENOENT);
    }
    return;
  } else if (r < 0) {
    derr << "error retrieving remote image id" << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_local_image_id->empty() &&
      m_local_image_tag_owner == librbd::Journal<>::LOCAL_MIRROR_UUID) {
    dout(5) << "local image is primary" << dendl;
    finish(-ENOMSG);
    return;
  }

  open_remote_image();
}

template <typename I>
void BootstrapRequest<I>::open_remote_image() {
  dout(15) << "remote_image_id=" << m_remote_image_id << dendl;

  update_progress("OPEN_REMOTE_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_remote_image>(
      this);
  OpenImageRequest<I> *request = OpenImageRequest<I>::create(
    m_remote_io_ctx, &m_remote_image_ctx, m_remote_image_id, false,
    ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_remote_image(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to open remote image: " << cpp_strerror(r) << dendl;
    ceph_assert(m_remote_image_ctx == nullptr);
    finish(r);
    return;
  }

  get_remote_mirror_info();
}

template <typename I>
void BootstrapRequest<I>::get_remote_mirror_info() {
  dout(15) << dendl;

  update_progress("GET_REMOTE_MIRROR_INFO");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_remote_mirror_info>(
      this);
  auto request = librbd::mirror::GetInfoRequest<I>::create(
    *m_remote_image_ctx, &m_mirror_image, &m_promotion_state, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_mirror_info(int r) {
  dout(15) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(5) << "remote image is not mirrored" << dendl;
    m_ret_val = -EREMOTEIO;
    close_remote_image();
    return;
  } else if (r < 0) {
    derr << "error querying remote image primary status: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  if (m_mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_DISABLING) {
    dout(5) << "remote image mirroring is being disabled" << dendl;
    m_ret_val = -EREMOTEIO;
    close_remote_image();
    return;
  }

  if (m_mirror_image.mode != cls::rbd::MIRROR_IMAGE_MODE_JOURNAL) {
    dout(5) << ": remote image is in unsupported mode: " << m_mirror_image.mode
            << dendl;
    m_ret_val = -EOPNOTSUPP;
    close_remote_image();
    return;
  }

  if (m_promotion_state != librbd::mirror::PROMOTION_STATE_PRIMARY &&
      m_local_image_id->empty()) {
    // no local image and remote isn't primary -- don't sync it
    dout(5) << "remote image is not primary -- not syncing"
            << dendl;
    m_ret_val = -EREMOTEIO;
    close_remote_image();
    return;
  }

  if (!m_client_meta.image_id.empty()) {
    // have an image id -- use that to open the image since a deletion (resync)
    // will leave the old image id registered in the peer
    *m_local_image_id = m_client_meta.image_id;
  }

  if (m_local_image_id->empty()) {
    create_local_image();
    return;
  }

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::open_local_image() {
  dout(15) << "local_image_id=" << *m_local_image_id << dendl;

  update_progress("OPEN_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_local_image>(
      this);
  OpenLocalImageRequest<I> *request = OpenLocalImageRequest<I>::create(
    m_local_io_ctx, m_local_image_ctx, *m_local_image_id, m_threads->work_queue,
    ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_local_image(int r) {
  dout(15) << "r=" << r << dendl;

  if (r == -ENOENT) {
    ceph_assert(*m_local_image_ctx == nullptr);
    dout(10) << "local image missing" << dendl;
    create_local_image();
    return;
  } else if (r == -EREMOTEIO) {
    ceph_assert(*m_local_image_ctx == nullptr);
    dout(10) << "local image is primary -- skipping image replay" << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  } else if (r < 0) {
    ceph_assert(*m_local_image_ctx == nullptr);
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

  // TODO support snapshot-based mirroring
  auto ctx = create_context_callback<
    BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_prepare_replay>(this);
  auto request = journal::PrepareReplayRequest<I>::create(
    *m_local_image_ctx, *m_remote_journaler, m_promotion_state,
    m_local_mirror_uuid, *m_remote_mirror_uuid, &m_client_meta,
    m_progress_ctx, m_do_resync, &m_syncing, ctx);
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
    close_local_image();
    return;
  } else if (*m_do_resync) {
    dout(10) << "local image resync requested" << dendl;
    close_remote_image();
    return;
  } else if (m_client_state == cls::journal::CLIENT_STATE_DISCONNECTED) {
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

  // TODO support snapshot-based mirroring
  auto ctx = create_context_callback<
    BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_create_local_image>(this);
  auto request = journal::CreateLocalImageRequest<I>::create(
    m_threads, m_local_io_ctx, m_remote_image_ctx, *m_remote_journaler,
    m_global_image_id, *m_remote_mirror_uuid, &m_client_meta, m_progress_ctx,
    m_local_image_id, ctx);
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

  m_client_state = cls::journal::CLIENT_STATE_CONNECTED;

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

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_image_sync>(this);
  m_image_sync = ImageSync<I>::create(
    *m_local_image_ctx, m_remote_image_ctx, m_threads->timer,
    &m_threads->timer_lock, m_local_mirror_uuid, *m_remote_journaler,
    &m_client_meta, m_threads->work_queue, m_instance_watcher, ctx,
    m_progress_ctx);
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
void BootstrapRequest<I>::close_local_image() {
  dout(15) << dendl;

  update_progress("CLOSE_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_close_local_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    m_local_image_ctx, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_local_image(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error encountered closing local image: " << cpp_strerror(r)
         << dendl;
  }

  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::close_remote_image() {
  dout(15) << dendl;

  update_progress("CLOSE_REMOTE_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_close_remote_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    &m_remote_image_ctx, ctx);
  request->send();
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
