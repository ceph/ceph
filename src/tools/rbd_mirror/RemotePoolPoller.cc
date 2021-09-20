// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "RemotePoolPoller.h"
#include "include/ceph_assert.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::RemotePollPoller: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {

static const double POLL_INTERVAL_SECONDS = 30;

using librbd::util::create_rados_callback;

template <typename I>
RemotePoolPoller<I>::~RemotePoolPoller() {
  ceph_assert(m_timer_task == nullptr);
}

template <typename I>
void RemotePoolPoller<I>::init(Context* on_finish) {
  dout(10) << dendl;

  ceph_assert(m_state == STATE_INITIALIZING);
  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;

  get_mirror_uuid();
}

template <typename I>
void RemotePoolPoller<I>::shut_down(Context* on_finish) {
  dout(10) << dendl;

  std::unique_lock locker(m_threads->timer_lock);
  ceph_assert(m_state == STATE_POLLING);
  m_state = STATE_SHUTTING_DOWN;

  if (m_timer_task == nullptr) {
    // currently executing a poll
    ceph_assert(m_on_finish == nullptr);
    m_on_finish = on_finish;
    return;
  }

  m_threads->timer->cancel_event(m_timer_task);
  m_timer_task = nullptr;
  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
void RemotePoolPoller<I>::get_mirror_uuid() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_uuid_get_start(&op);

  auto aio_comp = create_rados_callback<
    RemotePoolPoller<I>, &RemotePoolPoller<I>::handle_get_mirror_uuid>(this);
  m_out_bl.clear();
  int r = m_remote_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemotePoolPoller<I>::handle_get_mirror_uuid(int r) {
  dout(10) << "r=" << r << dendl;
  std::string remote_mirror_uuid;
  if (r >= 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_uuid_get_finish(&it, &remote_mirror_uuid);
    if (r >= 0 && remote_mirror_uuid.empty()) {
      r = -ENOENT;
    }
  }

  if (r < 0) {
    if (r == -ENOENT) {
      dout(5) << "remote mirror uuid missing" << dendl;
    } else {
      derr << "failed to retrieve remote mirror uuid: " << cpp_strerror(r)
           << dendl;
    }

    m_remote_pool_meta.mirror_uuid = "";
  }

  // if we have the mirror uuid, we will poll until shut down
  if (m_state == STATE_INITIALIZING) {
    if (r < 0) {
      schedule_task(r);
      return;
    }

    m_state = STATE_POLLING;
  }

  dout(10) << "remote_mirror_uuid=" << remote_mirror_uuid << dendl;
  if (m_remote_pool_meta.mirror_uuid != remote_mirror_uuid) {
    m_remote_pool_meta.mirror_uuid = remote_mirror_uuid;
    m_updated = true;
  }

  mirror_peer_ping();
}

template <typename I>
void RemotePoolPoller<I>::mirror_peer_ping() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_peer_ping(&op, m_site_name, m_local_mirror_uuid);

  auto aio_comp = create_rados_callback<
    RemotePoolPoller<I>, &RemotePoolPoller<I>::handle_mirror_peer_ping>(this);
  int r = m_remote_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemotePoolPoller<I>::handle_mirror_peer_ping(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -EOPNOTSUPP) {
    // older OSD that doesn't support snaphot-based mirroring, so no need
    // to query remote peers
    dout(10) << "remote peer does not support snapshot-based mirroring"
             << dendl;
    notify_listener();
    return;
  } else if (r < 0) {
    // we can still see if we can perform a peer list and find outselves
    derr << "failed to ping remote mirror peer: " << cpp_strerror(r) << dendl;
  }

  mirror_peer_list();
}

template <typename I>
void RemotePoolPoller<I>::mirror_peer_list() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_peer_list_start(&op);

  auto aio_comp = create_rados_callback<
    RemotePoolPoller<I>, &RemotePoolPoller<I>::handle_mirror_peer_list>(this);
  m_out_bl.clear();
  int r = m_remote_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemotePoolPoller<I>::handle_mirror_peer_list(int r) {
  dout(10) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_peer_list_finish(&iter, &peers);
  }

  if (r < 0) {
    derr << "failed to retrieve mirror peers: " << cpp_strerror(r) << dendl;
  }

  cls::rbd::MirrorPeer* matched_peer = nullptr;
  for (auto& peer : peers) {
    if (peer.mirror_peer_direction == cls::rbd::MIRROR_PEER_DIRECTION_RX) {
      continue;
    }

    if (peer.mirror_uuid == m_local_mirror_uuid) {
      matched_peer = &peer;
      break;
    } else if (peer.site_name == m_site_name) {
      // keep searching in case we hit an exact match by fsid
      matched_peer = &peer;
    }
  }

  // older OSDs don't support peer ping so we might fail to find a match,
  // which will prevent snapshot mirroring from functioning
  std::string remote_mirror_peer_uuid;
  if (matched_peer != nullptr) {
    remote_mirror_peer_uuid = matched_peer->uuid;
  }

  dout(10) << "remote_mirror_peer_uuid=" << remote_mirror_peer_uuid << dendl;
  if (m_remote_pool_meta.mirror_peer_uuid != remote_mirror_peer_uuid) {
    m_remote_pool_meta.mirror_peer_uuid = remote_mirror_peer_uuid;
    m_updated = true;
  }

  notify_listener();
}

template <typename I>
void RemotePoolPoller<I>::notify_listener() {
  bool updated = false;
  std::swap(updated, m_updated);
  if (updated) {
    dout(10) << dendl;
    m_listener.handle_updated(m_remote_pool_meta);
  }

  schedule_task(0);
}

template <typename I>
void RemotePoolPoller<I>::schedule_task(int r) {
  std::unique_lock locker{m_threads->timer_lock};

  if (m_state == STATE_POLLING) {
    dout(10) << dendl;

    ceph_assert(m_timer_task == nullptr);
    m_timer_task = new LambdaContext([this](int) {
      handle_task();
    });

    m_threads->timer->add_event_after(POLL_INTERVAL_SECONDS, m_timer_task);
  }

  // finish init or shut down callback
  if (m_on_finish != nullptr) {
    locker.unlock();
    Context* on_finish = nullptr;
    std::swap(on_finish, m_on_finish);
    on_finish->complete(m_state == STATE_SHUTTING_DOWN ? 0 : r);
  }
}

template <typename I>
void RemotePoolPoller<I>::handle_task() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked_by_me(m_threads->timer_lock));
  m_timer_task = nullptr;

  auto ctx = new LambdaContext([this](int) {
    get_mirror_uuid();
  });
  m_threads->work_queue->queue(ctx);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::RemotePoolPoller<librbd::ImageCtx>;
