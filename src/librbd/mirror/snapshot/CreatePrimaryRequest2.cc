// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/CreatePrimaryRequest2.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/Utils.h"

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::CreatePrimaryRequest2: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
CreatePrimaryRequest2<I>::CreatePrimaryRequest2(
    std::vector<I *>&image_ctxs, std::vector<std::string> &global_image_ids,
    std::vector<uint64_t> &clean_since_snap_ids, uint64_t snap_create_flags,
    uint32_t flags, const std::string &group_snap_id,
    std::vector<uint64_t>&snap_ids, Context *on_finish)
  : m_image_ctxs(image_ctxs), m_global_image_ids(global_image_ids),
    m_clean_since_snap_ids(clean_since_snap_ids),
    m_snap_create_flags(snap_create_flags), m_flags(flags),
    m_group_snap_id(group_snap_id), m_snap_ids(snap_ids),
    m_on_finish(on_finish) {

  if (m_group_snap_id.empty() && m_image_ctxs.size() > 1) {
    finish(-EINVAL);
    return;
  }

  m_cct = m_image_ctxs[0]->cct;
//FIXME: using the first image_ctx pool for now
  m_default_ns_ctx.dup(m_image_ctxs[0]->md_ctx);
  m_default_ns_ctx.set_namespace("");
}

template <typename I>
void CreatePrimaryRequest2<I>::send() {
  size_t i = 0;
  for (; i < m_image_ctxs.size(); i++) {
    if (!util::can_create_primary_snapshot(
          m_image_ctxs[i],
	  ((m_flags & CREATE_PRIMARY_FLAG_DEMOTED) != 0),
	  ((m_flags & CREATE_PRIMARY_FLAG_FORCE) != 0), nullptr, nullptr)) {

    lderr(m_cct) << "cannot create primary snapshot for " << m_image_ctxs[i]->id
               << dendl;

      finish(-EINVAL);
      return;
    }
  }

  m_snap_names.resize(m_image_ctxs.size());

  for (i = 0; i < m_image_ctxs.size(); i++) {
    std::stringstream ss;
    ss << ".mirror.primary." << m_global_image_ids[i] << ".";
    if (!m_group_snap_id.empty()) {
      ss << m_image_ctxs[i]->group_spec.pool_id << "_"
	 << m_image_ctxs[i]->group_spec.group_id << "_"
	<< m_group_snap_id;
    } else {
      uuid_d uuid_gen;
      uuid_gen.generate_random();
      ss << uuid_gen.to_string();
    }
    m_snap_names[i] = ss.str();
  }
  get_mirror_peers();
}

template <typename I>
void CreatePrimaryRequest2<I>::get_mirror_peers() {
  ldout(m_cct, 15) << dendl;

//FIXME : mirror peers will be different for images in different pools
// For now using the same mirror peers for all images
// by assuming that all images are in the same pool

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  librados::AioCompletion *comp = create_rados_callback<
    CreatePrimaryRequest2<I>,
    &CreatePrimaryRequest2<I>::handle_get_mirror_peers>(this);
  m_out_bl.clear();
  int r = m_default_ns_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void CreatePrimaryRequest2<I>::handle_get_mirror_peers(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_peer_list_finish(&iter, &peers);
  }

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror peers: " << cpp_strerror(r)
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
    lderr(m_cct) << "no mirror tx peers configured for the pool" << dendl;
    finish(-EINVAL);
    return;
  }

  notify_quiesce();
}

template <typename I>
void CreatePrimaryRequest2<I>::create_snapshots() {

  auto ctx = create_context_callback<
    CreatePrimaryRequest2<I>,
    &CreatePrimaryRequest2<I>::handle_create_snapshots>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    cls::rbd::MirrorSnapshotNamespace ns{
      ((m_flags & CREATE_PRIMARY_FLAG_DEMOTED) != 0 ?
	cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED :
	cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY),
	m_mirror_peer_uuids, "", m_clean_since_snap_ids[i]};
    ns.group_spec = m_image_ctxs[i]->group_spec;
    ns.group_snap_id = m_group_snap_id;

    ldout(m_cct, 15) << "name=" << m_snap_names[i] << ", "
                     << "ns=" << ns << dendl;

    m_image_ctxs[i]->operations->snap_create(ns, m_snap_names[i],
                                             m_snap_create_flags,
                                             m_prog_ctx, gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void CreatePrimaryRequest2<I>::handle_create_snapshots(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create mirror snapshot: " << cpp_strerror(r)
               << dendl;
    if (m_ret_code == 0) {
      m_ret_code = r;
    }
  //Refresh the images anyway so we can return any available snap_ids.
  }

  refresh_images();
}

template <typename I>
void CreatePrimaryRequest2<I>::refresh_images() {
  // refresh is required to retrieve the snapshot id (if snapshot
  // created via remote RPC) and complete flag (regardless)
  ldout(m_cct, 15) << dendl;

  auto ctx = create_context_callback<
    CreatePrimaryRequest2<I>,
    &CreatePrimaryRequest2<I>::handle_refresh_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_image_ctxs[i]->state->refresh(gather_ctx->new_sub());
  }
  gather_ctx->activate();
}

template <typename I>
void CreatePrimaryRequest2<I>::handle_refresh_images(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (!m_group_snap_id.empty()) {
    for (size_t i = 0; i < m_image_ctxs.size(); i++) {
      std::shared_lock image_locker{m_image_ctxs[i]->image_lock};

      ldout(m_cct, 15) << "snap_name=" << m_snap_names[i] << dendl;
      auto snap_id = m_image_ctxs[i]->get_snap_id(
        cls::rbd::MirrorSnapshotNamespace{}, m_snap_names[i]);
      m_snap_ids[i] = snap_id;
      ldout(m_cct, 15) << "image_id: " <<  m_image_ctxs[i]->id
                       << ", snap_id=" << snap_id << dendl;
    }
  }

  if (r < 0) {
    lderr(m_cct) << "failed to refresh images: " << cpp_strerror(r) << dendl;
    // TO DO : if refresh failed, there may be a leftover mirror snapshot
    release_exclusive_locks();
    return;
  }
  // Do not unlink snaps if the images are part of a group
  if (!m_group_snap_id.empty()) {
    release_exclusive_locks();
    return;
  }

  unlink_peer();
}

template <typename I>
void CreatePrimaryRequest2<I>::unlink_peer() {
  // TODO: Document semantics for unlink_peer
  uint64_t max_snapshots = m_image_ctxs[0]->config.template get_val<uint64_t>(
    "rbd_mirroring_max_mirroring_snapshots");
  ceph_assert(max_snapshots >= 3);

  std::string peer_uuid;
  uint64_t snap_id = CEPH_NOSNAP;

  {
    std::shared_lock image_locker{m_image_ctxs[0]->image_lock};
    for (const auto& peer : m_mirror_peer_uuids) {
      for (const auto& snap_info_pair : m_image_ctxs[0]->snap_info) {
        auto info = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
          &snap_info_pair.second.snap_namespace);
        if (info == nullptr) {
          continue;
        }
        if (info->mirror_peer_uuids.empty() ||
            (info->mirror_peer_uuids.count(peer) != 0 &&
             info->is_primary() && !info->complete)) {
          if (info->group_spec.is_valid() || !info->group_snap_id.empty()) {
            // snap is part of a group snap
            continue;
          }
          peer_uuid = peer;
          snap_id = snap_info_pair.first;
          goto do_unlink;
        }
      }
    }
    for (const auto& peer : m_mirror_peer_uuids) {
      size_t count = 0;
      uint64_t unlink_snap_id = 0;
      for (const auto& snap_info_pair : m_image_ctxs[0]->snap_info) {
        auto info = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
          &snap_info_pair.second.snap_namespace);
        if (info == nullptr) {
          continue;
        }
        if (info->state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
          // reset counters -- we count primary snapshots after the last
          // promotion
          count = 0;
          unlink_snap_id = 0;
          continue;
        }
        if (info->mirror_peer_uuids.count(peer) == 0) {
          // snapshot is not linked with this peer
          continue;
        }
        if (info->group_spec.is_valid() || !info->group_snap_id.empty()) {
          // snap is part of a group snap
          continue;
        }
        count++;
        if (count == max_snapshots) {
          unlink_snap_id = snap_info_pair.first;
        }
        if (count > max_snapshots) {
          peer_uuid = peer;
          snap_id = unlink_snap_id;
          goto do_unlink;
        }
      }
    }
  }

  finish(0);
  return;

do_unlink:
  ldout(m_cct, 15) << "peer=" << peer_uuid << ", snap_id=" << snap_id << dendl;

  auto ctx = create_context_callback<
    CreatePrimaryRequest2<I>,
    &CreatePrimaryRequest2<I>::handle_unlink_peer>(this);
  auto req = UnlinkPeerRequest<I>::create(m_image_ctxs[0], snap_id, peer_uuid, true,
                                          ctx);
  req->send();
}

template <typename I>
void CreatePrimaryRequest2<I>::handle_unlink_peer(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unlink peer: " << cpp_strerror(r) << dendl;
    finish(0); // not fatal
    return;
  }

  unlink_peer();
}


template <typename I>
void CreatePrimaryRequest2<I>::notify_quiesce() {

  if (m_group_snap_id.empty()) {
    create_snapshots();
    return;
  }
  if ((m_snap_create_flags & SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE) != 0) {
    acquire_exclusive_locks();
    return;
  }
  ldout(m_cct, 15) << dendl;

  // The individual image snaps do not need to quiesce.
  m_snap_create_flags |= SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE;
  auto ctx = create_context_callback<
    CreatePrimaryRequest2<I>,
    &CreatePrimaryRequest2<I>::handle_notify_quiesce>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  int image_count = m_image_ctxs.size();
  m_quiesce_requests.resize(image_count);

  for (int i = 0; i < image_count; ++i) {
    auto ictx = (m_image_ctxs)[i];
    ictx->image_watcher->notify_quiesce(&(m_quiesce_requests)[i], m_prog_ctx,
                                        gather_ctx->new_sub());
  }

  gather_ctx->activate();

}


template <typename I>
void CreatePrimaryRequest2<I>::handle_notify_quiesce(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0 &&
      (m_snap_create_flags & SNAP_CREATE_FLAG_IGNORE_NOTIFY_QUIESCE_ERROR) == 0) {
    m_ret_code = r;
    notify_unquiesce();
    return;
  }
  acquire_exclusive_locks();

}


template <typename I>
void CreatePrimaryRequest2<I>::notify_unquiesce() {

  if (m_quiesce_requests.empty()) {
    finish(m_ret_code);
    return;
  }

  ldout(m_cct, 10) << dendl;

  ceph_assert(m_quiesce_requests.size() == m_image_ctxs.size());

  auto ctx = librbd::util::create_context_callback<
    CreatePrimaryRequest2<I>,
    &CreatePrimaryRequest2<I>::handle_notify_unquiesce>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  int image_count = m_image_ctxs.size();
  for (int i = 0; i < image_count; ++i) {
    auto ictx = m_image_ctxs[i];
    ictx->image_watcher->notify_unquiesce(m_quiesce_requests[i],
                                          gather_ctx->new_sub());
  }

  gather_ctx->activate();

}

template <typename I>
void CreatePrimaryRequest2<I>::handle_notify_unquiesce(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unquiesce requests: "
                 << cpp_strerror(r) << dendl;
  }
  finish(m_ret_code);
}

template <typename I>
void CreatePrimaryRequest2<I>::acquire_exclusive_locks() {
  ldout(m_cct, 15) << dendl;

  m_release_locks = true;

  auto ctx = librbd::util::create_context_callback<
    CreatePrimaryRequest2<I>,
    &CreatePrimaryRequest2<I>::handle_acquire_exclusive_locks>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    std::shared_lock owner_lock{ictx->owner_lock};
    if (ictx->exclusive_lock != nullptr) {
      ictx->exclusive_lock->block_requests(-EBUSY);
      ictx->exclusive_lock->acquire_lock(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void CreatePrimaryRequest2<I>::handle_acquire_exclusive_locks(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to acquire image exclusive locks: "
                 << cpp_strerror(r) << dendl;
    m_ret_code = r;
    notify_unquiesce();
    return;
  }

  create_snapshots();
}

template <typename I>
void CreatePrimaryRequest2<I>::release_exclusive_locks() {
  ldout(m_cct, 15) << dendl;

  if(!m_release_locks){
    notify_unquiesce();
    return;
  }
  auto ctx = librbd::util::create_context_callback<
    CreatePrimaryRequest2<I>,
    &CreatePrimaryRequest2<I>::handle_release_exclusive_locks>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    std::shared_lock owner_lock{ictx->owner_lock};
    if (ictx->exclusive_lock != nullptr) {
      ictx->exclusive_lock->release_lock(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();

}

template <typename I>
void CreatePrimaryRequest2<I>::handle_release_exclusive_locks(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to release exclusive locks for images: "
                 << cpp_strerror(r) << dendl;
  }
  notify_unquiesce();
}


template <typename I>
void CreatePrimaryRequest2<I>::finish(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::CreatePrimaryRequest2<librbd::ImageCtx>;
