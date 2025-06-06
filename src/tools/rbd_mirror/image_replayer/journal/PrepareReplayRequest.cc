// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PrepareReplayRequest.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ProgressContext.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"

#include <shared_mutex> // for std::shared_lock

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::journal::" \
                           << "PrepareReplayRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

using librbd::util::create_context_callback;

template <typename I>
void PrepareReplayRequest<I>::send() {
  *m_resync_requested = false;
  *m_syncing = false;

  if (m_state_builder->local_image_id !=
        m_state_builder->remote_client_meta.image_id) {
    // somehow our local image has a different image id than the image id
    // registered in the remote image
    derr << "split-brain detected: local_image_id="
         << m_state_builder->local_image_id << ", "
         << "registered local_image_id="
         << m_state_builder->remote_client_meta.image_id << dendl;
    finish(-EEXIST);
    return;
  }

  std::shared_lock image_locker(m_state_builder->local_image_ctx->image_lock);
  if (m_state_builder->local_image_ctx->journal == nullptr) {
    image_locker.unlock();

    derr << "local image does not support journaling" << dendl;
    finish(-EINVAL);
    return;
  }

  int r = m_state_builder->local_image_ctx->journal->is_resync_requested(
    m_resync_requested);
  if (r < 0) {
    image_locker.unlock();

    derr << "failed to check if a resync was requested" << dendl;
    finish(r);
    return;
  }

  m_local_tag_tid = m_state_builder->local_image_ctx->journal->get_tag_tid();
  m_local_tag_data = m_state_builder->local_image_ctx->journal->get_tag_data();
  dout(10) << "local tag=" << m_local_tag_tid << ", "
           << "local tag data=" << m_local_tag_data << dendl;
  image_locker.unlock();

  if (*m_resync_requested) {
    finish(0);
    return;
  } else if (m_state_builder->remote_client_meta.state ==
               librbd::journal::MIRROR_PEER_STATE_SYNCING &&
             m_local_tag_data.mirror_uuid ==
               m_state_builder->remote_mirror_uuid) {
    // if the initial sync hasn't completed, we cannot replay
    *m_syncing = true;
    finish(0);
    return;
  }

  update_client_state();
}

template <typename I>
void PrepareReplayRequest<I>::update_client_state() {
  if (m_state_builder->remote_client_meta.state !=
        librbd::journal::MIRROR_PEER_STATE_SYNCING ||
      m_local_tag_data.mirror_uuid == m_state_builder->remote_mirror_uuid) {
    get_remote_tag_class();
    return;
  }

  // our local image is not primary, is flagged as syncing on the remote side,
  // but is no longer tied to the remote -- this implies we were forced
  // promoted and then demoted at some point
  dout(15) << dendl;
  update_progress("UPDATE_CLIENT_STATE");

  auto client_meta = m_state_builder->remote_client_meta;
  client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  encode(client_data, data_bl);

  auto ctx = create_context_callback<
    PrepareReplayRequest<I>,
    &PrepareReplayRequest<I>::handle_update_client_state>(this);
  m_state_builder->remote_journaler->update_client(data_bl, ctx);
}

template <typename I>
void PrepareReplayRequest<I>::handle_update_client_state(int r) {
  dout(15) << "r=" << r << dendl;
  if (r < 0) {
    derr << "failed to update client: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_state_builder->remote_client_meta.state =
    librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  get_remote_tag_class();
}

template <typename I>
void PrepareReplayRequest<I>::get_remote_tag_class() {
  dout(10) << dendl;
  update_progress("GET_REMOTE_TAG_CLASS");

  auto ctx = create_context_callback<
    PrepareReplayRequest<I>,
    &PrepareReplayRequest<I>::handle_get_remote_tag_class>(this);
  m_state_builder->remote_journaler->get_client(
    librbd::Journal<>::IMAGE_CLIENT_ID, &m_client, ctx);
}

template <typename I>
void PrepareReplayRequest<I>::handle_get_remote_tag_class(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to retrieve remote client: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  librbd::journal::ClientData client_data;
  auto it = m_client.data.cbegin();
  try {
    decode(client_data, it);
  } catch (const buffer::error &err) {
    derr << "failed to decode remote client meta data: " << err.what()
         << dendl;
    finish(-EBADMSG);
    return;
  }

  librbd::journal::ImageClientMeta *client_meta =
    std::get_if<librbd::journal::ImageClientMeta>(&client_data.client_meta);
  if (client_meta == nullptr) {
    derr << "unknown remote client registration" << dendl;
    finish(-EINVAL);
    return;
  }

  m_remote_tag_class = client_meta->tag_class;
  dout(10) << "remote tag class=" << m_remote_tag_class << dendl;

  get_remote_tags();
}

template <typename I>
void PrepareReplayRequest<I>::get_remote_tags() {
  dout(10) << dendl;
  update_progress("GET_REMOTE_TAGS");

  auto ctx = create_context_callback<
    PrepareReplayRequest<I>,
    &PrepareReplayRequest<I>::handle_get_remote_tags>(this);
  m_state_builder->remote_journaler->get_tags(m_remote_tag_class,
                                              &m_remote_tags, ctx);
}

template <typename I>
void PrepareReplayRequest<I>::handle_get_remote_tags(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to retrieve remote tags: " << cpp_strerror(r) << dendl;
    finish(r);
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
        m_local_tag_data.predecessor.mirror_uuid ==
          m_state_builder->remote_mirror_uuid &&
        m_local_tag_data.predecessor.tag_tid > remote_tag.tid) {
      dout(10) << "skipping processed predecessor remote tag "
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
      finish(-EBADMSG);
      return;
    }

    dout(10) << "decoded remote tag " << remote_tag.tid << ": "
             << remote_tag_data << dendl;

    if (!m_local_tag_data.predecessor.commit_valid) {
      // newly synced local image (no predecessor) replays from the first tag
      if (remote_tag_data.mirror_uuid != librbd::Journal<>::LOCAL_MIRROR_UUID) {
        dout(10) << "skipping non-primary remote tag" << dendl;
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
          dout(10) << "found matching local demotion tag" << dendl;
          remote_orphan_tag_tid = remote_tag.tid;
          continue;
        }

        if (m_local_tag_data.predecessor.mirror_uuid ==
              m_state_builder->remote_mirror_uuid &&
            remote_tag_data.predecessor.mirror_uuid ==
              librbd::Journal<>::LOCAL_MIRROR_UUID) {
          // remote demoted and local has matching event
          dout(10) << "found matching remote demotion tag" << dendl;
          remote_orphan_tag_tid = remote_tag.tid;
          continue;
        }
      }

      if (remote_tag_data.mirror_uuid == librbd::Journal<>::LOCAL_MIRROR_UUID &&
          remote_tag_data.predecessor.mirror_uuid ==
            librbd::Journal<>::ORPHAN_MIRROR_UUID &&
          remote_tag_data.predecessor.commit_valid && remote_orphan_tag_tid &&
          remote_tag_data.predecessor.tag_tid == *remote_orphan_tag_tid) {
        // remote promotion tag chained to remote/local demotion tag
        dout(10) << "found chained remote promotion tag" << dendl;
        reconnect_orphan = true;
        break;
      }

      // promotion must follow demotion
      remote_orphan_tag_tid = boost::none;
    }
  }

  if (remote_tag_data_valid &&
      m_local_tag_data.mirror_uuid == m_state_builder->remote_mirror_uuid) {
    dout(10) << "local image is in clean replay state" << dendl;
  } else if (reconnect_orphan) {
    dout(10) << "remote image was demoted/promoted" << dendl;
  } else {
    derr << "split-brain detected -- skipping image replay" << dendl;
    finish(-EEXIST);
    return;
  }

  finish(0);
}

template <typename I>
void PrepareReplayRequest<I>::update_progress(const std::string &description) {
  dout(10) << description << dendl;

  if (m_progress_ctx != nullptr) {
    m_progress_ctx->update_progress(description);
  }
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::journal::PrepareReplayRequest<librbd::ImageCtx>;
