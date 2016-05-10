// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SnapshotCopyRequest.h"
#include "SnapshotCreateRequest.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/journal/Types.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_sync::SnapshotCopyRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_sync {

namespace {

template <typename I>
const std::string &get_snapshot_name(I *image_ctx, librados::snap_t snap_id) {
  auto snap_it = std::find_if(image_ctx->snap_ids.begin(),
                              image_ctx->snap_ids.end(),
                              [snap_id](
      const std::pair<std::string, librados::snap_t> &pair) {
    return pair.second == snap_id;
  });
  assert(snap_it != image_ctx->snap_ids.end());
  return snap_it->first;
}

} // anonymous namespace

using librbd::util::create_context_callback;

template <typename I>
SnapshotCopyRequest<I>::SnapshotCopyRequest(I *local_image_ctx,
                                            I *remote_image_ctx,
                                            SnapMap *snap_map,
                                            Journaler *journaler,
                                            librbd::journal::MirrorPeerClientMeta *meta,
                                            Context *on_finish)
  : m_local_image_ctx(local_image_ctx), m_remote_image_ctx(remote_image_ctx),
    m_snap_map(snap_map), m_journaler(journaler), m_client_meta(meta),
    m_on_finish(on_finish), m_snap_seqs(meta->snap_seqs) {
  m_snap_map->clear();

  // snap ids ordered from oldest to newest
  m_remote_snap_ids.insert(remote_image_ctx->snaps.begin(),
                           remote_image_ctx->snaps.end());
  m_local_snap_ids.insert(local_image_ctx->snaps.begin(),
                          local_image_ctx->snaps.end());
}

template <typename I>
void SnapshotCopyRequest<I>::send() {
  send_snap_unprotect();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_unprotect() {
  CephContext *cct = m_local_image_ctx->cct;

  // TODO: issue #14937 needs to add support for cloned images
  m_remote_image_ctx->snap_lock.get_read();
  if (m_remote_image_ctx->parent_md.spec.pool_id != -1 ||
      std::find_if(m_remote_image_ctx->snap_info.begin(),
                   m_remote_image_ctx->snap_info.end(),
                   [](const std::pair<librados::snap_t, librbd::SnapInfo>& pair) {
          return pair.second.parent.spec.pool_id != -1;
        }) != m_remote_image_ctx->snap_info.end()) {
    lderr(cct) << ": cloned images are not currently supported" << dendl;
    m_remote_image_ctx->snap_lock.put_read();
    finish(-EINVAL);
    return;
  }
  m_remote_image_ctx->snap_lock.put_read();

  SnapIdSet::iterator snap_id_it = m_local_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_local_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_local_snap_ids.end(); ++snap_id_it) {
    librados::snap_t local_snap_id = *snap_id_it;

    m_local_image_ctx->snap_lock.get_read();
    bool local_unprotected;
    int r = m_local_image_ctx->is_snap_unprotected(local_snap_id,
                                                   &local_unprotected);
    if (r < 0) {
      lderr(cct) << "failed to retrieve local snap unprotect status: "
                 << cpp_strerror(r) << dendl;
      m_local_image_ctx->snap_lock.put_read();
      finish(r);
      return;
    }
    m_local_image_ctx->snap_lock.put_read();

    if (local_unprotected) {
      // snap is already unprotected -- check next snap
      continue;
    }

    // if local snapshot is protected and (1) it isn't in our mapping
    // table, or (2) the remote snapshot isn't protected, unprotect it
    auto snap_seq_it = std::find_if(
      m_snap_seqs.begin(), m_snap_seqs.end(),
      [local_snap_id](const SnapSeqs::value_type& pair) {
        return pair.second == local_snap_id;
      });

    if (snap_seq_it != m_snap_seqs.end()) {
      m_remote_image_ctx->snap_lock.get_read();
      bool remote_unprotected;
      r = m_remote_image_ctx->is_snap_unprotected(snap_seq_it->first,
                                                  &remote_unprotected);
      if (r < 0) {
        lderr(cct) << "failed to retrieve remote snap unprotect status: "
                   << cpp_strerror(r) << dendl;
        m_remote_image_ctx->snap_lock.put_read();
        finish(r);
        return;
      }
      m_remote_image_ctx->snap_lock.put_read();

      if (remote_unprotected) {
        // remote is unprotected -- unprotect local snap
        break;
      }
    } else {
      // remote snapshot doesn't exist -- unprotect local snap
      break;
    }
  }

  if (snap_id_it == m_local_snap_ids.end()) {
    // no local snapshots to unprotect
    m_prev_snap_id = CEPH_NOSNAP;
    send_snap_remove();
    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_local_image_ctx, m_prev_snap_id);

  ldout(cct, 20) << ": "
                 << "snap_name=" << m_snap_name << ", "
                 << "snap_id=" << m_prev_snap_id << dendl;

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_snap_unprotect>(
      this);
  RWLock::RLocker owner_locker(m_local_image_ctx->owner_lock);
  m_local_image_ctx->operations->execute_snap_unprotect(m_snap_name.c_str(),
                                                        ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_unprotect(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << ": failed to unprotect snapshot '" << m_snap_name << "': "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_snap_unprotect();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_remove() {
  CephContext *cct = m_local_image_ctx->cct;

  SnapIdSet::iterator snap_id_it = m_local_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_local_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_local_snap_ids.end(); ++snap_id_it) {
    librados::snap_t local_snap_id = *snap_id_it;

    // if the local snapshot isn't in our mapping table, remove it
    auto snap_seq_it = std::find_if(
      m_snap_seqs.begin(), m_snap_seqs.end(),
      [local_snap_id](const SnapSeqs::value_type& pair) {
        return pair.second == local_snap_id;
      });

    if (snap_seq_it == m_snap_seqs.end()) {
      break;
    }
  }

  if (snap_id_it == m_local_snap_ids.end()) {
    // no local snapshots to delete
    m_prev_snap_id = CEPH_NOSNAP;
    send_snap_create();
    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_local_image_ctx, m_prev_snap_id);

  ldout(cct, 20) << ": "
                 << "snap_name=" << m_snap_name << ", "
                 << "snap_id=" << m_prev_snap_id << dendl;

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_snap_remove>(
      this);
  RWLock::RLocker owner_locker(m_local_image_ctx->owner_lock);
  m_local_image_ctx->operations->execute_snap_remove(m_snap_name.c_str(), ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_remove(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << ": failed to remove snapshot '" << m_snap_name << "': "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_snap_remove();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_create() {
  CephContext *cct = m_local_image_ctx->cct;

  SnapIdSet::iterator snap_id_it = m_remote_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_remote_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_remote_snap_ids.end(); ++snap_id_it) {
    librados::snap_t remote_snap_id = *snap_id_it;

    // if the remote snapshot isn't in our mapping table, create it
    if (m_snap_seqs.find(remote_snap_id) == m_snap_seqs.end()) {
      break;
    }
  }

  if (snap_id_it == m_remote_snap_ids.end()) {
    // no remote snapshots to create
    m_prev_snap_id = CEPH_NOSNAP;
    send_snap_protect();
    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_remote_image_ctx, m_prev_snap_id);

  m_remote_image_ctx->snap_lock.get_read();
  auto snap_info_it = m_remote_image_ctx->snap_info.find(m_prev_snap_id);
  if (snap_info_it == m_remote_image_ctx->snap_info.end()) {
    m_remote_image_ctx->snap_lock.put_read();
    lderr(cct) << "failed to retrieve remote snap info: " << m_snap_name
               << dendl;
    finish(-ENOENT);
    return;
  }
  uint64_t size = snap_info_it->second.size;
  m_remote_image_ctx->snap_lock.put_read();

  ldout(cct, 20) << ": "
                 << "snap_name=" << m_snap_name << ", "
                 << "snap_id=" << m_prev_snap_id << ", "
                 << "size=" << size << dendl;

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_snap_create>(
      this);
  SnapshotCreateRequest<I> *req = SnapshotCreateRequest<I>::create(
    m_local_image_ctx, m_snap_name, size, ctx);
  req->send();
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_create(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << ": failed to create snapshot '" << m_snap_name << "': "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  assert(m_prev_snap_id != CEPH_NOSNAP);

  auto snap_it = m_local_image_ctx->snap_ids.find(m_snap_name);
  assert(snap_it != m_local_image_ctx->snap_ids.end());
  librados::snap_t local_snap_id = snap_it->second;

  ldout(cct, 20) << ": mapping remote snap id " << m_prev_snap_id << " to "
                 << local_snap_id << dendl;
  m_snap_seqs[m_prev_snap_id] = local_snap_id;

  send_snap_create();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_protect() {
  CephContext *cct = m_local_image_ctx->cct;

  SnapIdSet::iterator snap_id_it = m_remote_snap_ids.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_id_it = m_remote_snap_ids.upper_bound(m_prev_snap_id);
  }

  for (; snap_id_it != m_remote_snap_ids.end(); ++snap_id_it) {
    librados::snap_t remote_snap_id = *snap_id_it;

    m_remote_image_ctx->snap_lock.get_read();
    bool remote_protected;
    int r = m_remote_image_ctx->is_snap_protected(remote_snap_id,
                                                  &remote_protected);
    if (r < 0) {
      lderr(cct) << "failed to retrieve remote snap protect status: "
                 << cpp_strerror(r) << dendl;
      m_remote_image_ctx->snap_lock.put_read();
      finish(r);
      return;
    }
    m_remote_image_ctx->snap_lock.put_read();

    if (!remote_protected) {
      // snap is not protected -- check next snap
      continue;
    }

    // if local snapshot is not protected, protect it
    auto snap_seq_it = m_snap_seqs.find(remote_snap_id);
    assert(snap_seq_it != m_snap_seqs.end());

    m_local_image_ctx->snap_lock.get_read();
    bool local_protected;
    r = m_local_image_ctx->is_snap_protected(snap_seq_it->second,
                                             &local_protected);
    if (r < 0) {
      lderr(cct) << "failed to retrieve local snap protect status: "
                 << cpp_strerror(r) << dendl;
      m_local_image_ctx->snap_lock.put_read();
      finish(r);
      return;
    }
    m_local_image_ctx->snap_lock.put_read();

    if (!local_protected) {
      break;
    }
  }

  if (snap_id_it == m_remote_snap_ids.end()) {
    // no local snapshots to protect
    m_prev_snap_id = CEPH_NOSNAP;
    send_update_client();
    return;
  }

  m_prev_snap_id = *snap_id_it;
  m_snap_name = get_snapshot_name(m_remote_image_ctx, m_prev_snap_id);

  ldout(cct, 20) << ": "
                 << "snap_name=" << m_snap_name << ", "
                 << "snap_id=" << m_prev_snap_id << dendl;

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_snap_protect>(
      this);
  RWLock::RLocker owner_locker(m_local_image_ctx->owner_lock);
  m_local_image_ctx->operations->execute_snap_protect(m_snap_name.c_str(), ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_protect(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << ": failed to protect snapshot '" << m_snap_name << "': "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_snap_protect();
}

template <typename I>
void SnapshotCopyRequest<I>::send_update_client() {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << dendl;

  compute_snap_map();

  librbd::journal::MirrorPeerClientMeta client_meta(*m_client_meta);
  client_meta.snap_seqs = m_snap_seqs;

  librbd::journal::ClientData client_data(client_meta);
  bufferlist data_bl;
  ::encode(client_data, data_bl);

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_update_client>(
      this);
  m_journaler->update_client(data_bl, ctx);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_update_client(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << ": failed to update client data: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  m_client_meta->snap_seqs = m_snap_seqs;

  finish(0);
}

template <typename I>
void SnapshotCopyRequest<I>::finish(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r >= 0) {
    m_client_meta->snap_seqs = m_snap_seqs;
  }

  m_on_finish->complete(r);
  delete this;
}

template <typename I>
void SnapshotCopyRequest<I>::compute_snap_map() {
  SnapIds local_snap_ids;
  for (auto &pair : m_snap_seqs) {
    local_snap_ids.reserve(1 + local_snap_ids.size());
    local_snap_ids.insert(local_snap_ids.begin(), pair.second);
    m_snap_map->insert(std::make_pair(pair.first, local_snap_ids));
  }
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_sync::SnapshotCopyRequest<librbd::ImageCtx>;
