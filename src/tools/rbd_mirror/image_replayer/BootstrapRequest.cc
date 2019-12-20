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
#include "tools/rbd_mirror/image_replayer/journal/CreateLocalImageRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::BootstrapRequest: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
BootstrapRequest<I>::BootstrapRequest(
        Threads<I>* threads,
        librados::IoCtx &local_io_ctx,
        librados::IoCtx &remote_io_ctx,
        InstanceWatcher<I> *instance_watcher,
        I **local_image_ctx,
        const std::string &local_image_id,
        const std::string &remote_image_id,
        const std::string &global_image_id,
        const std::string &local_mirror_uuid,
        const std::string &remote_mirror_uuid,
        Journaler *remote_journaler,
        cls::journal::ClientState *client_state,
        MirrorPeerClientMeta *client_meta,
        Context *on_finish,
        bool *do_resync,
        rbd::mirror::ProgressContext *progress_ctx)
  : BaseRequest("rbd::mirror::image_replayer::BootstrapRequest",
		reinterpret_cast<CephContext*>(local_io_ctx.cct()), on_finish),
    m_threads(threads), m_local_io_ctx(local_io_ctx),
    m_remote_io_ctx(remote_io_ctx), m_instance_watcher(instance_watcher),
    m_local_image_ctx(local_image_ctx), m_local_image_id(local_image_id),
    m_remote_image_id(remote_image_id), m_global_image_id(global_image_id),
    m_local_mirror_uuid(local_mirror_uuid),
    m_remote_mirror_uuid(remote_mirror_uuid),
    m_remote_journaler(remote_journaler),
    m_client_state(client_state), m_client_meta(client_meta),
    m_progress_ctx(progress_ctx), m_do_resync(do_resync),
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

  open_remote_image();
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
      m_local_image_id.empty()) {
    // no local image and remote isn't primary -- don't sync it
    dout(5) << "remote image is not primary -- not syncing"
            << dendl;
    m_ret_val = -EREMOTEIO;
    close_remote_image();
    return;
  }

  if (!m_client_meta->image_id.empty()) {
    // have an image id -- use that to open the image since a deletion (resync)
    // will leave the old image id registered in the peer
    m_local_image_id = m_client_meta->image_id;
  }

  if (m_local_image_id.empty()) {
    create_local_image();
    return;
  }

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::open_local_image() {
  dout(15) << "local_image_id=" << m_local_image_id << dendl;

  update_progress("OPEN_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_local_image>(
      this);
  OpenLocalImageRequest<I> *request = OpenLocalImageRequest<I>::create(
    m_local_io_ctx, m_local_image_ctx, m_local_image_id, m_threads->work_queue,
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

  I *local_image_ctx = (*m_local_image_ctx);
  {
    local_image_ctx->image_lock.lock_shared();
    if (local_image_ctx->journal == nullptr) {
      local_image_ctx->image_lock.unlock_shared();

      derr << "local image does not support journaling" << dendl;
      m_ret_val = -EINVAL;
      close_local_image();
      return;
    }

    r = (*m_local_image_ctx)->journal->is_resync_requested(m_do_resync);
    if (r < 0) {
      local_image_ctx->image_lock.unlock_shared();

      derr << "failed to check if a resync was requested" << dendl;
      m_ret_val = r;
      close_local_image();
      return;
    }

    m_local_tag_tid = local_image_ctx->journal->get_tag_tid();
    m_local_tag_data = local_image_ctx->journal->get_tag_data();
    dout(10) << "local tag=" << m_local_tag_tid << ", "
             << "local tag data=" << m_local_tag_data << dendl;
    local_image_ctx->image_lock.unlock_shared();
  }

  if (m_local_tag_data.mirror_uuid != m_remote_mirror_uuid &&
      m_promotion_state != librbd::mirror::PROMOTION_STATE_PRIMARY) {
    // if the local mirror is not linked to the (now) non-primary image,
    // stop the replay. Otherwise, we ignore that the remote is non-primary
    // so that we can replay the demotion
    dout(5) << "remote image is not primary -- skipping image replay"
            << dendl;
    m_ret_val = -EREMOTEIO;
    close_local_image();
    return;
  }

  if (*m_do_resync) {
    close_remote_image();
    return;
  }

  if (*m_client_state == cls::journal::CLIENT_STATE_DISCONNECTED) {
    dout(10) << "client flagged disconnected -- skipping bootstrap" << dendl;
    // The caller is expected to detect disconnect initializing remote journal.
    m_ret_val = 0;
    close_remote_image();
    return;
  }

  update_client_state();
}

template <typename I>
void BootstrapRequest<I>::update_client_state() {
  if (m_client_meta->state != librbd::journal::MIRROR_PEER_STATE_SYNCING ||
      m_local_tag_data.mirror_uuid == m_remote_mirror_uuid) {
    get_remote_tag_class();
    return;
  }

  // our local image is not primary, is flagged as syncing on the remote side,
  // but is no longer tied to the remote -- this implies we were forced
  // promoted and then demoted at some point
  dout(15) << dendl;
  update_progress("UPDATE_CLIENT_STATE");

  librbd::journal::MirrorPeerClientMeta client_meta(*m_client_meta);
  client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_update_client_state>(
      this);
  m_remote_journaler->update_client(data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_update_client_state(int r) {
  dout(15) << "r=" << r << dendl;
  if (r < 0) {
    derr << "failed to update client: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_local_image();
    return;
  }

  m_client_meta->state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  get_remote_tag_class();
}

template <typename I>
void BootstrapRequest<I>::create_local_image() {
  update_progress("CREATE_LOCAL_IMAGE");

  // TODO support snapshot-based mirroring
  auto ctx = create_context_callback<
    BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_create_local_image>(this);
  auto request = journal::CreateLocalImageRequest<I>::create(
    m_threads, m_local_io_ctx, m_remote_image_ctx, m_remote_journaler,
    m_global_image_id, m_remote_mirror_uuid, m_client_meta, m_progress_ctx,
    &m_local_image_id, ctx);
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

  *m_client_state = cls::journal::CLIENT_STATE_CONNECTED;

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::get_remote_tag_class() {
  if (m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_SYNCING) {
    // optimization -- no need to compare remote tags if we just created
    // the image locally or sync was interrupted
    image_sync();
    return;
  }

  dout(15) << dendl;

  update_progress("GET_REMOTE_TAG_CLASS");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_remote_tag_class>(
      this);
  m_remote_journaler->get_client(librbd::Journal<>::IMAGE_CLIENT_ID, &m_client, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_tag_class(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to retrieve remote client: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_local_image();
    return;
  }

  librbd::journal::ClientData client_data;
  auto it = m_client.data.cbegin();
  try {
    decode(client_data, it);
  } catch (const buffer::error &err) {
    derr << "failed to decode remote client meta data: " << err.what()
         << dendl;
    m_ret_val = -EBADMSG;
    close_local_image();
    return;
  }

  librbd::journal::ImageClientMeta *client_meta =
    boost::get<librbd::journal::ImageClientMeta>(&client_data.client_meta);
  if (client_meta == nullptr) {
    derr << "unknown remote client registration" << dendl;
    m_ret_val = -EINVAL;
    close_local_image();
    return;
  }

  m_remote_tag_class = client_meta->tag_class;
  dout(10) << "remote tag class=" << m_remote_tag_class << dendl;

  get_remote_tags();
}

template <typename I>
void BootstrapRequest<I>::get_remote_tags() {
  dout(15) << dendl;
  update_progress("GET_REMOTE_TAGS");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_remote_tags>(this);
  m_remote_journaler->get_tags(m_remote_tag_class, &m_remote_tags, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_tags(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to retrieve remote tags: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_local_image();
    return;
  }

  if (m_canceled) {
    dout(10) << "request canceled" << dendl;
    m_ret_val = -ECANCELED;
    close_local_image();
    return;
  }

  // At this point, the local image was existing, non-primary, and replaying;
  // and the remote image is primary.  Attempt to link the local image's most
  // recent tag to the remote image's tag chain.
  bool remote_tag_data_valid = false;
  librbd::journal::TagData remote_tag_data;
  boost::optional<uint64_t> remote_orphan_tag_tid =
    boost::make_optional<uint64_t>(false, 0U);
  bool reconnect_orphan = false;

  // decode the remote tags
  for (auto &remote_tag : m_remote_tags) {
    if (m_local_tag_data.predecessor.commit_valid &&
        m_local_tag_data.predecessor.mirror_uuid == m_remote_mirror_uuid &&
        m_local_tag_data.predecessor.tag_tid > remote_tag.tid) {
      dout(15) << "skipping processed predecessor remote tag "
               << remote_tag.tid << dendl;
      continue;
    }

    try {
      auto it = remote_tag.data.cbegin();
      decode(remote_tag_data, it);
      remote_tag_data_valid = true;
    } catch (const buffer::error &err) {
      derr << "failed to decode remote tag " << remote_tag.tid << ": "
           << err.what() << dendl;
      m_ret_val = -EBADMSG;
      close_local_image();
      return;
    }

    dout(10) << "decoded remote tag " << remote_tag.tid << ": "
             << remote_tag_data << dendl;

    if (!m_local_tag_data.predecessor.commit_valid) {
      // newly synced local image (no predecessor) replays from the first tag
      if (remote_tag_data.mirror_uuid != librbd::Journal<>::LOCAL_MIRROR_UUID) {
        dout(15) << "skipping non-primary remote tag" << dendl;
        continue;
      }

      dout(10) << "using initial primary remote tag" << dendl;
      break;
    }

    if (m_local_tag_data.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID) {
      // demotion last available local epoch

      if (remote_tag_data.mirror_uuid == m_local_tag_data.mirror_uuid &&
          remote_tag_data.predecessor.commit_valid &&
          remote_tag_data.predecessor.tag_tid ==
            m_local_tag_data.predecessor.tag_tid) {
        // demotion matches remote epoch

        if (remote_tag_data.predecessor.mirror_uuid == m_local_mirror_uuid &&
            m_local_tag_data.predecessor.mirror_uuid ==
              librbd::Journal<>::LOCAL_MIRROR_UUID) {
          // local demoted and remote has matching event
          dout(15) << "found matching local demotion tag" << dendl;
          remote_orphan_tag_tid = remote_tag.tid;
          continue;
        }

        if (m_local_tag_data.predecessor.mirror_uuid == m_remote_mirror_uuid &&
            remote_tag_data.predecessor.mirror_uuid ==
              librbd::Journal<>::LOCAL_MIRROR_UUID) {
          // remote demoted and local has matching event
          dout(15) << "found matching remote demotion tag" << dendl;
          remote_orphan_tag_tid = remote_tag.tid;
          continue;
        }
      }

      if (remote_tag_data.mirror_uuid == librbd::Journal<>::LOCAL_MIRROR_UUID &&
          remote_tag_data.predecessor.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID &&
          remote_tag_data.predecessor.commit_valid && remote_orphan_tag_tid &&
          remote_tag_data.predecessor.tag_tid == *remote_orphan_tag_tid) {
        // remote promotion tag chained to remote/local demotion tag
        dout(15) << "found chained remote promotion tag" << dendl;
        reconnect_orphan = true;
        break;
      }

      // promotion must follow demotion
      remote_orphan_tag_tid = boost::none;
    }
  }

  if (remote_tag_data_valid &&
      m_local_tag_data.mirror_uuid == m_remote_mirror_uuid) {
    dout(10) << "local image is in clean replay state" << dendl;
  } else if (reconnect_orphan) {
    dout(10) << "remote image was demoted/promoted" << dendl;
  } else {
    derr << "split-brain detected -- skipping image replay" << dendl;
    m_ret_val = -EEXIST;
    close_local_image();
    return;
  }

  image_sync();
}

template <typename I>
void BootstrapRequest<I>::image_sync() {
  if (m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_REPLAYING) {
    // clean replay state -- no image sync required
    close_remote_image();
    return;
  }

  {
    std::unique_lock locker{m_lock};
    if (m_canceled) {
      m_ret_val = -ECANCELED;
    } else {
      dout(15) << dendl;
      ceph_assert(m_image_sync == nullptr);

      Context *ctx = create_context_callback<
        BootstrapRequest<I>, &BootstrapRequest<I>::handle_image_sync>(this);
      m_image_sync = ImageSync<I>::create(
          *m_local_image_ctx, m_remote_image_ctx, m_threads->timer,
          &m_threads->timer_lock, m_local_mirror_uuid, m_remote_journaler,
          m_client_meta, m_threads->work_queue, m_instance_watcher, ctx,
          m_progress_ctx);

      m_image_sync->get();

      locker.unlock();
      update_progress("IMAGE_SYNC");
      locker.lock();

      m_image_sync->send();
      return;
    }
  }

  dout(10) << "request canceled" << dendl;
  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::handle_image_sync(int r) {
  dout(15) << "r=" << r << dendl;

  {
    std::lock_guard locker{m_lock};
    m_image_sync->put();
    m_image_sync = nullptr;

    if (m_canceled) {
      dout(10) << "request canceled" << dendl;
      m_ret_val = -ECANCELED;
    }

    if (r < 0) {
      derr << "failed to sync remote image: " << cpp_strerror(r) << dendl;
      m_ret_val = r;
    }
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
