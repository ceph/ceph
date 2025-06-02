// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/Utils.h"

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::GroupImageCreatePrimaryRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;
using librbd::util::snap_create_flags_api_to_internal;
using librbd::util::get_default_snap_create_flags;

template <typename I>
GroupImageCreatePrimaryRequest<I>::GroupImageCreatePrimaryRequest(
    CephContext* cct, const std::vector<I *> &image_ctxs,
    const std::vector<std::string> &global_image_ids,
    uint64_t group_snap_create_flags, uint32_t flags,
    const std::string &group_snap_id, std::vector<uint64_t> *snap_ids,
    Context *on_finish)
  : m_cct(cct), m_image_ctxs(image_ctxs), m_global_image_ids(global_image_ids),
    m_group_snap_create_flags(group_snap_create_flags), m_flags(flags),
    m_group_snap_id(group_snap_id), m_snap_ids(snap_ids),
    m_on_finish(on_finish) {
  ceph_assert(!m_image_ctxs.empty());
  ceph_assert(!m_group_snap_id.empty());
  ceph_assert(m_global_image_ids.size() == m_image_ctxs.size());
  ceph_assert((*m_snap_ids).size() == m_image_ctxs.size());
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::send() {
  ldout(m_cct, 15) << dendl;

  size_t i = 0;
  for (; i < m_image_ctxs.size(); i++) {
    if (!util::can_create_primary_snapshot(
          m_image_ctxs[i],
          ((m_flags & CREATE_PRIMARY_FLAG_DEMOTED) != 0),
          ((m_flags & CREATE_PRIMARY_FLAG_FORCE) != 0), nullptr, nullptr)) {
      lderr(m_cct) << "cannot create primary snapshot for "
                   << m_image_ctxs[i]->id << dendl;
      finish(-EINVAL);
      return;
    }
  }

  m_snap_names.resize(m_image_ctxs.size());

  for (i = 0; i < m_image_ctxs.size(); i++) {
    std::stringstream ss;
    ss << ".mirror.primary." << m_global_image_ids[i] << "."
       << m_image_ctxs[i]->group_spec.pool_id << "_"
       << m_image_ctxs[i]->group_spec.group_id << "_"
       << m_group_snap_id;
    m_snap_names[i] = ss.str();
  }

  get_mirror_peers();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::get_mirror_peers() {
  ldout(m_cct, 15) << dendl;

  auto ctx = create_context_callback<
    GroupImageCreatePrimaryRequest<I>,
    &GroupImageCreatePrimaryRequest<I>::handle_get_mirror_peers>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  m_default_ns_ctxs.resize(m_image_ctxs.size());
  m_mirror_peers_uuids.resize(m_image_ctxs.size());
  m_out_bls.resize(m_image_ctxs.size());

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_default_ns_ctxs[i].dup(m_image_ctxs[i]->md_ctx);
    m_default_ns_ctxs[i].set_namespace("");

    librados::ObjectReadOperation op;
    cls_client::mirror_peer_list_start(&op);

    auto on_mirror_peer_list = new LambdaContext(
      [this, i, new_sub_ctx = gather_ctx->new_sub()](int r) {
        std::vector<cls::rbd::MirrorPeer> peers;
        if (r == 0) {
          auto iter = m_out_bls[i].cbegin();
          r = cls_client::mirror_peer_list_finish(&iter, &peers);
        }

        if (r < 0) {
          lderr(m_image_ctxs[i]->cct) << "failed to retrieve mirror peers: "
                                      << cpp_strerror(r) << dendl;
        } else {
          for (auto &peer : peers) {
            if (peer.mirror_peer_direction ==
                cls::rbd::MIRROR_PEER_DIRECTION_RX) {
              continue;
            }
            m_mirror_peers_uuids[i].insert(peer.uuid);
          }

          if (m_mirror_peers_uuids[i].empty() &&
              ((m_flags & CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS) == 0)) {
            lderr(m_image_ctxs[i]->cct) << "no mirror tx peers configured "
                                        << "for the pool" << dendl;
            r = -EINVAL;
          }
        }

        new_sub_ctx->complete(r);
      });

    auto comp = create_rados_callback(on_mirror_peer_list);

    int r = m_default_ns_ctxs[i].aio_operate(RBD_MIRRORING, comp, &op,
                                             &m_out_bls[i]);
    ceph_assert(r == 0);
    comp->release();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::handle_get_mirror_peers(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  m_default_ns_ctxs.clear();
  m_out_bls.clear();

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror peers for images: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if ((m_group_snap_create_flags &
       SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE) != 0) {
    acquire_exclusive_locks();
    return;
  }

  notify_quiesce();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::notify_quiesce() {
  ldout(m_cct, 15) << dendl;

  auto ctx = create_context_callback<
    GroupImageCreatePrimaryRequest<I>,
    &GroupImageCreatePrimaryRequest<I>::handle_notify_quiesce>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  m_quiesce_requests.resize(m_image_ctxs.size());

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    auto ictx = m_image_ctxs[i];
    ictx->image_watcher->notify_quiesce(&(m_quiesce_requests)[i], m_prog_ctx,
                                        gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::handle_notify_quiesce(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0 &&
      (m_group_snap_create_flags & SNAP_CREATE_FLAG_IGNORE_NOTIFY_QUIESCE_ERROR) == 0) {
    m_ret_code = r;
    notify_unquiesce();
    return;
  }

  acquire_exclusive_locks();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::acquire_exclusive_locks() {
  ldout(m_cct, 15) << dendl;

  m_release_locks = true;

  auto ctx = librbd::util::create_context_callback<
    GroupImageCreatePrimaryRequest<I>,
    &GroupImageCreatePrimaryRequest<I>::handle_acquire_exclusive_locks>(this);
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
void GroupImageCreatePrimaryRequest<I>::handle_acquire_exclusive_locks(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to acquire image exclusive locks: "
                 << cpp_strerror(r) << dendl;
    m_ret_code = r;
    // release locks in case some of the lock acquisitions succeeded
    release_exclusive_locks();
    return;
  }

  create_snapshots();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::create_snapshots() {
  ldout(m_cct, 15) << dendl;

  auto ctx = create_context_callback<
    GroupImageCreatePrimaryRequest<I>,
    &GroupImageCreatePrimaryRequest<I>::handle_create_snapshots>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    cls::rbd::MirrorSnapshotNamespace ns{
      ((m_flags & CREATE_PRIMARY_FLAG_DEMOTED) != 0 ?
        cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED :
        cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY),
      m_mirror_peers_uuids[i], "", CEPH_NOSNAP};
    ns.group_spec = m_image_ctxs[i]->group_spec;
    ns.group_snap_id = m_group_snap_id;

    ldout(m_cct, 15) << "creating snapshot: image_id=" << m_image_ctxs[i]->id
                     << ", snap_name=" << m_snap_names[i]
                     << ", snap_ns=" << ns << dendl;

    uint64_t snap_create_flags;
    int r = snap_create_flags_api_to_internal(
      m_cct, get_default_snap_create_flags(m_image_ctxs[i]),
      &snap_create_flags);
    ceph_assert(r == 0);

    m_image_ctxs[i]->operations->snap_create(ns, m_snap_names[i],
                                             snap_create_flags, m_prog_ctx,
                                             gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::handle_create_snapshots(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create image snapshots: " << cpp_strerror(r)
                 << dendl;
    m_ret_code = r;
    // Refresh the images anyway so we can return any available snap_ids
  }

  refresh_images();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::refresh_images() {
  // Refresh is required to retrieve the snapshot id (if snapshot
  // created via remote RPC) and complete flag (regardless)
  ldout(m_cct, 15) << dendl;

  auto ctx = create_context_callback<
    GroupImageCreatePrimaryRequest<I>,
    &GroupImageCreatePrimaryRequest<I>::handle_refresh_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto on_refresh = new LambdaContext(
      [this, i, new_sub_ctx = gather_ctx->new_sub()](int r) {
        auto cct = m_image_ctxs[i]->cct;
        if (r < 0) {
          lderr(cct) << "failed to refresh image: " << cpp_strerror(r)
                     << dendl;
        } else {
          ldout(cct, 15) << "snap_name=" << m_snap_names[i] << dendl;

          std::shared_lock image_locker{m_image_ctxs[i]->image_lock};

          auto snap_id = m_image_ctxs[i]->get_snap_id(
            cls::rbd::MirrorSnapshotNamespace{}, m_snap_names[i]);
          (*m_snap_ids)[i] = snap_id;
          ldout(cct, 15) << "image_id: " <<  m_image_ctxs[i]->id
                         << ", snap_id=" << snap_id << dendl;
        }

        new_sub_ctx->complete(r);
      });

    m_image_ctxs[i]->state->refresh(on_refresh);
  }
  gather_ctx->activate();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::handle_refresh_images(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to refresh images: " << cpp_strerror(r) << dendl;
    if (m_ret_code == 0) {
      m_ret_code = r;
    }
  }

  if (m_release_locks) {
    release_exclusive_locks();
  } else {
    notify_unquiesce();
  }
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::release_exclusive_locks() {
  ldout(m_cct, 15) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupImageCreatePrimaryRequest<I>,
    &GroupImageCreatePrimaryRequest<I>::handle_release_exclusive_locks>(this);
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
void GroupImageCreatePrimaryRequest<I>::handle_release_exclusive_locks(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to release exclusive locks for images: "
                 << cpp_strerror(r) << dendl;
    if (m_ret_code == 0) {
      m_ret_code = r;
    }
  }

  notify_unquiesce();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::notify_unquiesce() {
  if (m_quiesce_requests.empty()) {
    finish(m_ret_code);
    return;
  }

  ldout(m_cct, 15) << dendl;

  ceph_assert(m_quiesce_requests.size() == m_image_ctxs.size());

  auto ctx = librbd::util::create_context_callback<
    GroupImageCreatePrimaryRequest<I>,
    &GroupImageCreatePrimaryRequest<I>::handle_notify_unquiesce>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    auto ictx = m_image_ctxs[i];
    ictx->image_watcher->notify_unquiesce(m_quiesce_requests[i],
                                          gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::handle_notify_unquiesce(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unquiesce requests: "
                 << cpp_strerror(r) << dendl;
    if (m_ret_code == 0) {
      m_ret_code = r;
    }
  }

  finish(m_ret_code);
}

template <typename I>
void GroupImageCreatePrimaryRequest<I>::finish(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::GroupImageCreatePrimaryRequest<librbd::ImageCtx>;
