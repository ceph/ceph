// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/Utils.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::CreatePrimaryRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
CreatePrimaryRequest<I>::CreatePrimaryRequest(
    I *image_ctx, const std::string& global_image_id,
    uint64_t clean_since_snap_id, uint32_t flags, uint64_t *snap_id,
    Context *on_finish)
  : m_image_ctx(image_ctx), m_global_image_id(global_image_id),
    m_clean_since_snap_id(clean_since_snap_id), m_flags(flags),
    m_snap_id(snap_id), m_on_finish(on_finish) {
  m_default_ns_ctx.dup(m_image_ctx->md_ctx);
  m_default_ns_ctx.set_namespace("");
}

template <typename I>
void CreatePrimaryRequest<I>::send() {
  if (!util::can_create_primary_snapshot(
        m_image_ctx,
        ((m_flags & CREATE_PRIMARY_FLAG_DEMOTED) != 0),
        ((m_flags & CREATE_PRIMARY_FLAG_FORCE) != 0), nullptr, nullptr)) {
    finish(-EINVAL);
    return;
  }

  uuid_d uuid_gen;
  uuid_gen.generate_random();
  m_snap_name = ".mirror.primary." + m_global_image_id + "." +
    uuid_gen.to_string();

  get_mirror_peers();
}

template <typename I>
void CreatePrimaryRequest<I>::get_mirror_peers() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  librados::AioCompletion *comp = create_rados_callback<
    CreatePrimaryRequest<I>,
    &CreatePrimaryRequest<I>::handle_get_mirror_peers>(this);
  m_out_bl.clear();
  int r = m_default_ns_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void CreatePrimaryRequest<I>::handle_get_mirror_peers(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_peer_list_finish(&iter, &peers);
  }

  if (r < 0) {
    lderr(cct) << "failed to retrieve mirror peers: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  for (auto &peer : peers) {
    if (peer.mirror_peer_direction == cls::rbd::MIRROR_PEER_DIRECTION_RX) {
      continue;
    }
    m_mirror_peer_uuids.insert(peer.uuid);
  }

  if (m_mirror_peer_uuids.empty() &&
      ((m_flags & CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS) == 0)) {
    lderr(cct) << "no mirror tx peers configured for the pool" << dendl;
    finish(-EINVAL);
    return;
  }

  create_snapshot();
}

template <typename I>
void CreatePrimaryRequest<I>::create_snapshot() {
  cls::rbd::MirrorSnapshotNamespace ns{
    ((m_flags & CREATE_PRIMARY_FLAG_DEMOTED) != 0 ?
      cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED :
      cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY),
    m_mirror_peer_uuids, "", m_clean_since_snap_id};

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "name=" << m_snap_name << ", "
                 << "ns=" << ns << dendl;
  auto ctx = create_context_callback<
    CreatePrimaryRequest<I>,
    &CreatePrimaryRequest<I>::handle_create_snapshot>(this);
  m_image_ctx->operations->snap_create(ns, m_snap_name, ctx);
}

template <typename I>
void CreatePrimaryRequest<I>::handle_create_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to create mirror snapshot: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  refresh_image();
}

template <typename I>
void CreatePrimaryRequest<I>::refresh_image() {
  // if snapshot created via remote RPC, refresh is required to retrieve
  // the snapshot id
  if (m_snap_id == nullptr) {
    unlink_peer();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    CreatePrimaryRequest<I>,
    &CreatePrimaryRequest<I>::handle_refresh_image>(this);
  m_image_ctx->state->refresh(ctx);
}

template <typename I>
void CreatePrimaryRequest<I>::handle_refresh_image(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    *m_snap_id = m_image_ctx->get_snap_id(
      cls::rbd::MirrorSnapshotNamespace{}, m_snap_name);
    ldout(cct, 15) << "snap_id=" << *m_snap_id << dendl;
  }

  unlink_peer();
}

template <typename I>
void CreatePrimaryRequest<I>::unlink_peer() {
  uint64_t max_snapshots = m_image_ctx->config.template get_val<uint64_t>(
    "rbd_mirroring_max_mirroring_snapshots");
  ceph_assert(max_snapshots >= 3);

  std::string peer_uuid;
  uint64_t snap_id = CEPH_NOSNAP;

  for (auto &peer : m_mirror_peer_uuids) {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    size_t count = 0;
    uint64_t unlink_snap_id = 0;
    for (auto &snap_it : m_image_ctx->snap_info) {
      auto info = boost::get<cls::rbd::MirrorSnapshotNamespace>(
        &snap_it.second.snap_namespace);
      if (info == nullptr) {
        continue;
      }
      if (info->state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
        // reset counters -- we count primary snapshots after the last promotion
        count = 0;
        unlink_snap_id = 0;
        continue;
      }
      count++;
      if (count == 3) {
        unlink_snap_id = snap_it.first;
      }
      if (count > max_snapshots) {
        peer_uuid = peer;
        snap_id = unlink_snap_id;
        break;
      }
    }
    if (snap_id != CEPH_NOSNAP) {
      break;
    }
  }

  if (snap_id == CEPH_NOSNAP) {
    finish(0);
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "peer=" << peer_uuid << ", snap_id=" << snap_id << dendl;

  auto ctx = create_context_callback<
    CreatePrimaryRequest<I>,
    &CreatePrimaryRequest<I>::handle_unlink_peer>(this);
  auto req = UnlinkPeerRequest<I>::create(m_image_ctx, snap_id, peer_uuid, ctx);
  req->send();
}

template <typename I>
void CreatePrimaryRequest<I>::handle_unlink_peer(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to unlink peer: " << cpp_strerror(r) << dendl;
    finish(0); // not fatal
    return;
  }

  unlink_peer();
}

template <typename I>
void CreatePrimaryRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::CreatePrimaryRequest<librbd::ImageCtx>;
