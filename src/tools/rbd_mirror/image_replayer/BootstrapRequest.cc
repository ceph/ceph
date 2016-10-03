// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/ProgressContext.h"
#include "tools/rbd_mirror/ImageSyncThrottler.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::BootstrapRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_ack_callback;
using librbd::util::unique_lock_name;

template <typename I>
BootstrapRequest<I>::BootstrapRequest(
        librados::IoCtx &local_io_ctx,
        librados::IoCtx &remote_io_ctx,
        std::shared_ptr<ImageSyncThrottler<I>> image_sync_throttler,
        I **local_image_ctx,
        const std::string &local_image_name,
        const std::string &remote_image_id,
        const std::string &global_image_id,
        ContextWQ *work_queue, SafeTimer *timer,
        Mutex *timer_lock,
        const std::string &local_mirror_uuid,
        const std::string &remote_mirror_uuid,
        Journaler *journaler,
        MirrorPeerClientMeta *client_meta,
        Context *on_finish,
        rbd::mirror::ProgressContext *progress_ctx)
  : BaseRequest("rbd::mirror::image_replayer::BootstrapRequest",
		reinterpret_cast<CephContext*>(local_io_ctx.cct()), on_finish),
    m_local_io_ctx(local_io_ctx), m_remote_io_ctx(remote_io_ctx),
    m_image_sync_throttler(image_sync_throttler),
    m_local_image_ctx(local_image_ctx), m_local_image_name(local_image_name),
    m_remote_image_id(remote_image_id), m_global_image_id(global_image_id),
    m_work_queue(work_queue), m_timer(timer), m_timer_lock(timer_lock),
    m_local_mirror_uuid(local_mirror_uuid),
    m_remote_mirror_uuid(remote_mirror_uuid), m_journaler(journaler),
    m_client_meta(client_meta), m_progress_ctx(progress_ctx),
    m_lock(unique_lock_name("BootstrapRequest::m_lock", this)) {
}

template <typename I>
BootstrapRequest<I>::~BootstrapRequest() {
  assert(m_remote_image_ctx == nullptr);
}

template <typename I>
void BootstrapRequest<I>::send() {
  get_local_image_id();
}

template <typename I>
void BootstrapRequest<I>::cancel() {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);
  m_canceled = true;

  m_image_sync_throttler->cancel_sync(m_local_io_ctx, m_local_image_id);
}

template <typename I>
void BootstrapRequest<I>::get_local_image_id() {
  dout(20) << dendl;

  update_progress("GET_LOCAL_IMAGE_ID");

  // attempt to cross-reference a local image by the global image id
  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_image_id_start(&op, m_global_image_id);

  librados::AioCompletion *aio_comp = create_rados_ack_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_local_image_id>(
      this);
  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_local_image_id(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    r = librbd::cls_client::mirror_image_get_image_id_finish(
      &iter, &m_local_image_id);
  }

  if (r == -ENOENT) {
    dout(10) << ": image not registered locally" << dendl;
  } else if (r < 0) {
    derr << ": failed to retrieve local image id: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_remote_tag_class();
}

template <typename I>
void BootstrapRequest<I>::get_remote_tag_class() {
  dout(20) << dendl;

  update_progress("GET_REMOTE_TAG_CLASS");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_remote_tag_class>(
      this);
  m_journaler->get_client(librbd::Journal<>::IMAGE_CLIENT_ID, &m_client, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_tag_class(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to retrieve remote client: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  librbd::journal::ClientData client_data;
  bufferlist::iterator it = m_client.data.begin();
  try {
    ::decode(client_data, it);
  } catch (const buffer::error &err) {
    derr << ": failed to decode remote client meta data: " << err.what()
         << dendl;
    finish(-EBADMSG);
    return;
  }

  librbd::journal::ImageClientMeta *client_meta =
    boost::get<librbd::journal::ImageClientMeta>(&client_data.client_meta);
  if (client_meta == nullptr) {
    derr << ": unknown remote client registration" << dendl;
    finish(-EINVAL);
    return;
  }

  m_remote_tag_class = client_meta->tag_class;
  dout(10) << ": remote tag class=" << m_remote_tag_class << dendl;

  get_client();
}

template <typename I>
void BootstrapRequest<I>::get_client() {
  dout(20) << dendl;

  update_progress("GET_CLIENT");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_client>(
      this);
  m_journaler->get_client(m_local_mirror_uuid, &m_client, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_get_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << ": client not registered" << dendl;
  } else if (r < 0) {
    derr << ": failed to retrieve client: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (decode_client_meta()) {
    // skip registration if it already exists
    open_remote_image();
    return;
  }

  register_client();
}

template <typename I>
void BootstrapRequest<I>::register_client() {
  dout(20) << dendl;

  update_progress("REGISTER_CLIENT");

  // record an place-holder record
  librbd::journal::ClientData client_data{
    librbd::journal::MirrorPeerClientMeta{m_local_image_id}};
  bufferlist client_data_bl;
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_register_client>(
      this);
  m_journaler->register_client(client_data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_register_client(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to register with remote journal: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  *m_client_meta = librbd::journal::MirrorPeerClientMeta(m_local_image_id);
  open_remote_image();
}

template <typename I>
void BootstrapRequest<I>::open_remote_image() {
  dout(20) << dendl;

  update_progress("OPEN_REMOTE_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_remote_image>(
      this);
  OpenImageRequest<I> *request = OpenImageRequest<I>::create(
    m_remote_io_ctx, &m_remote_image_ctx, m_remote_image_id, false,
    m_work_queue, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_remote_image(int r) {
  // deduce the class type for the journal to support unit tests
  typedef typename std::decay<decltype(*I::journal)>::type Journal;

  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to open remote image: " << cpp_strerror(r) << dendl;
    assert(m_remote_image_ctx == nullptr);
    finish(r);
    return;
  }

  // TODO: make async
  bool tag_owner;
  r = Journal::is_tag_owner(m_remote_image_ctx, &tag_owner);
  if (r < 0) {
    derr << ": failed to query remote image primary status: " << cpp_strerror(r)
         << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  if (!tag_owner) {
    dout(5) << ": remote image is not primary -- skipping image replay"
            << dendl;
    m_ret_val = -EREMOTEIO;
    update_client_state();
    return;
  }

  // default local image name to the remote image name if not provided
  if (m_local_image_name.empty()) {
    m_local_image_name = m_remote_image_ctx->name;
  }

  if (m_local_image_id.empty()) {
    create_local_image();
    return;
  }

  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::update_client_state() {
  if (m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_REPLAYING) {
    // state already set for replaying upon failover
    close_remote_image();
    return;
  }

  dout(20) << dendl;
  update_progress("UPDATE_CLIENT_STATE");

  librbd::journal::MirrorPeerClientMeta client_meta(*m_client_meta);
  client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  ::encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_update_client_state>(
      this);
  m_journaler->update_client(data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_update_client_state(int r) {
  dout(20) << ": r=" << r << dendl;
  if (r < 0) {
    derr << ": failed to update client: " << cpp_strerror(r) << dendl;
  } else {
    m_client_meta->state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;;
  }

  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::open_local_image() {
  dout(20) << dendl;

  update_progress("OPEN_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_open_local_image>(
      this);
  OpenLocalImageRequest<I> *request = OpenLocalImageRequest<I>::create(
    m_local_io_ctx, m_local_image_ctx,
    (!m_local_image_id.empty() ? std::string() : m_local_image_name),
    m_local_image_id, m_work_queue, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_open_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -ENOENT) {
    assert(*m_local_image_ctx == nullptr);
    dout(10) << ": local image missing" << dendl;
    create_local_image();
    return;
  } else if (r == -EREMOTEIO) {
    assert(*m_local_image_ctx == nullptr);
    dout(10) << "local image is primary -- skipping image replay" << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  } else if (r < 0) {
    assert(*m_local_image_ctx == nullptr);
    derr << ": failed to open local image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  update_client_image();
}

template <typename I>
void BootstrapRequest<I>::create_local_image() {
  dout(20) << dendl;

  m_local_image_id = "";
  update_progress("CREATE_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_create_local_image>(
      this);
  CreateImageRequest<I> *request = CreateImageRequest<I>::create(
    m_local_io_ctx, m_work_queue, m_global_image_id, m_remote_mirror_uuid,
    m_local_image_name, m_remote_image_ctx, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to create local image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_remote_image();
    return;
  }

  m_created_local_image = true;
  open_local_image();
}

template <typename I>
void BootstrapRequest<I>::update_client_image() {
  dout(20) << dendl;

  update_progress("UPDATE_CLIENT_IMAGE");

  if (m_client_meta->image_id == (*m_local_image_ctx)->id) {
    // already registered local image with remote journal
    get_remote_tags();
    return;
  }
  m_local_image_id = (*m_local_image_ctx)->id;

  dout(20) << dendl;

  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.image_id = m_local_image_id;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  ::encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_update_client_image>(
      this);
  m_journaler->update_client(data_bl, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_update_client_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update client: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_local_image();
    return;
  }

  if (m_canceled) {
    dout(10) << ": request canceled" << dendl;
    m_ret_val = -ECANCELED;
    close_local_image();
    return;
  }

  m_client_meta->image_id = m_local_image_id;
  get_remote_tags();
}

template <typename I>
void BootstrapRequest<I>::get_remote_tags() {
  dout(20) << dendl;

  update_progress("GET_REMOTE_TAGS");

  if (m_created_local_image ||
      m_client_meta->state == librbd::journal::MIRROR_PEER_STATE_SYNCING) {
    // optimization -- no need to compare remote tags if we just created
    // the image locally or sync was interrupted
    image_sync();
    return;
  }

  dout(20) << dendl;

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_get_remote_tags>(this);
  m_journaler->get_tags(m_remote_tag_class, &m_remote_tags, ctx);
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_tags(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to retrieve remote tags: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_local_image();
    return;
  }

  if (m_canceled) {
    dout(10) << ": request canceled" << dendl;
    m_ret_val = -ECANCELED;
    close_local_image();
    return;
  }

  // At this point, the local image was existing, non-primary, and replaying;
  // and the remote image is primary.  Attempt to link the local image's most
  // recent tag to the remote image's tag chain.
  uint64_t local_tag_tid;
  librbd::journal::TagData local_tag_data;
  I *local_image_ctx = (*m_local_image_ctx);
  {
    RWLock::RLocker snap_locker(local_image_ctx->snap_lock);
    if (local_image_ctx->journal == nullptr) {
      derr << "local image does not support journaling" << dendl;
      m_ret_val = -EINVAL;
      close_local_image();
      return;
    }

    local_tag_tid = local_image_ctx->journal->get_tag_tid();
    local_tag_data = local_image_ctx->journal->get_tag_data();
    dout(20) << ": local tag " << local_tag_tid << ": "
             << local_tag_data << dendl;
  }

  bool remote_tag_data_valid = false;
  librbd::journal::TagData remote_tag_data;
  boost::optional<uint64_t> remote_orphan_tag_tid =
    boost::make_optional<uint64_t>(false, 0U);
  bool reconnect_orphan = false;

  // decode the remote tags
  for (auto &remote_tag : m_remote_tags) {
    if (local_tag_data.predecessor.commit_valid &&
        local_tag_data.predecessor.mirror_uuid == m_remote_mirror_uuid &&
        local_tag_data.predecessor.tag_tid > remote_tag.tid) {
      dout(20) << ": skipping processed predecessor remote tag "
               << remote_tag.tid << dendl;
      continue;
    }

    try {
      bufferlist::iterator it = remote_tag.data.begin();
      ::decode(remote_tag_data, it);
      remote_tag_data_valid = true;
    } catch (const buffer::error &err) {
      derr << ": failed to decode remote tag " << remote_tag.tid << ": "
           << err.what() << dendl;
      m_ret_val = -EBADMSG;
      close_local_image();
      return;
    }

    dout(10) << ": decoded remote tag " << remote_tag.tid << ": "
             << remote_tag_data << dendl;

    if (!local_tag_data.predecessor.commit_valid) {
      // newly synced local image (no predecessor) replays from the first tag
      if (remote_tag_data.mirror_uuid != librbd::Journal<>::LOCAL_MIRROR_UUID) {
        dout(20) << ": skipping non-primary remote tag" << dendl;
        continue;
      }

      dout(20) << ": using initial primary remote tag" << dendl;
      break;
    }

    if (local_tag_data.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID) {
      // demotion last available local epoch

      if (remote_tag_data.mirror_uuid == local_tag_data.mirror_uuid &&
          remote_tag_data.predecessor.commit_valid &&
          remote_tag_data.predecessor.tag_tid ==
            local_tag_data.predecessor.tag_tid) {
        // demotion matches remote epoch

        if (remote_tag_data.predecessor.mirror_uuid == m_local_mirror_uuid &&
            local_tag_data.predecessor.mirror_uuid ==
              librbd::Journal<>::LOCAL_MIRROR_UUID) {
          // local demoted and remote has matching event
          dout(20) << ": found matching local demotion tag" << dendl;
          remote_orphan_tag_tid = remote_tag.tid;
          continue;
        }

        if (local_tag_data.predecessor.mirror_uuid == m_remote_mirror_uuid &&
            remote_tag_data.predecessor.mirror_uuid ==
              librbd::Journal<>::LOCAL_MIRROR_UUID) {
          // remote demoted and local has matching event
          dout(20) << ": found matching remote demotion tag" << dendl;
          remote_orphan_tag_tid = remote_tag.tid;
          continue;
        }
      }

      if (remote_tag_data.mirror_uuid == librbd::Journal<>::LOCAL_MIRROR_UUID &&
          remote_tag_data.predecessor.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID &&
          remote_tag_data.predecessor.commit_valid && remote_orphan_tag_tid &&
          remote_tag_data.predecessor.tag_tid == *remote_orphan_tag_tid) {
        // remote promotion tag chained to remote/local demotion tag
        dout(20) << ": found chained remote promotion tag" << dendl;
        reconnect_orphan = true;
        break;
      }

      // promotion must follow demotion
      remote_orphan_tag_tid = boost::none;
    }
  }

  if (remote_tag_data_valid &&
      local_tag_data.mirror_uuid == m_remote_mirror_uuid) {
    dout(20) << ": local image is in clean replay state" << dendl;
  } else if (reconnect_orphan) {
    dout(20) << ": remote image was demoted/promoted" << dendl;
  } else {
    derr << ": split-brain detected -- skipping image replay" << dendl;
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

  dout(20) << dendl;
  update_progress("IMAGE_SYNC");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_image_sync>(
      this);

  {
    Mutex::Locker locker(m_lock);
    if (!m_canceled) {
      m_image_sync_throttler->start_sync(*m_local_image_ctx,
                                         m_remote_image_ctx, m_timer,
                                         m_timer_lock,
                                         m_local_mirror_uuid, m_journaler,
                                         m_client_meta, m_work_queue, ctx,
                                         m_progress_ctx);
      return;
    }
  }

  dout(10) << ": request canceled" << dendl;
  m_ret_val = -ECANCELED;
  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::handle_image_sync(int r) {
  dout(20) << ": r=" << r << dendl;

  if (m_canceled) {
    dout(10) << ": request canceled" << dendl;
    m_ret_val = -ECANCELED;
  }

  if (r < 0) {
    derr << ": failed to sync remote image: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::close_local_image() {
  dout(20) << dendl;

  update_progress("CLOSE_LOCAL_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_close_local_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    m_local_image_ctx, m_work_queue, false, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_local_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": error encountered closing local image: " << cpp_strerror(r)
         << dendl;
  }

  close_remote_image();
}

template <typename I>
void BootstrapRequest<I>::close_remote_image() {
  dout(20) << dendl;

  update_progress("CLOSE_REMOTE_IMAGE");

  Context *ctx = create_context_callback<
    BootstrapRequest<I>, &BootstrapRequest<I>::handle_close_remote_image>(
      this);
  CloseImageRequest<I> *request = CloseImageRequest<I>::create(
    &m_remote_image_ctx, m_work_queue, false, ctx);
  request->send();
}

template <typename I>
void BootstrapRequest<I>::handle_close_remote_image(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": error encountered closing remote image: " << cpp_strerror(r)
         << dendl;
  }

  finish(m_ret_val);
}

template <typename I>
bool BootstrapRequest<I>::decode_client_meta() {
  dout(20) << dendl;

  librbd::journal::ClientData client_data;
  bufferlist::iterator it = m_client.data.begin();
  try {
    ::decode(client_data, it);
  } catch (const buffer::error &err) {
    derr << ": failed to decode client meta data: " << err.what() << dendl;
    return true;
  }

  librbd::journal::MirrorPeerClientMeta *client_meta =
    boost::get<librbd::journal::MirrorPeerClientMeta>(&client_data.client_meta);
  if (client_meta == nullptr) {
    derr << ": unknown peer registration" << dendl;
    return true;
  } else if (!client_meta->image_id.empty()) {
    // have an image id -- use that to open the image
    m_local_image_id = client_meta->image_id;
  }

  *m_client_meta = *client_meta;

  dout(20) << ": client found: image_id=" << m_local_image_id
	   << ", client_meta=" << *m_client_meta << dendl;
  return true;
}

template <typename I>
void BootstrapRequest<I>::update_progress(const std::string &description) {
  dout(20) << ": " << description << dendl;

  if (m_progress_ctx) {
    m_progress_ctx->update_progress(description);
  }
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::BootstrapRequest<librbd::ImageCtx>;
