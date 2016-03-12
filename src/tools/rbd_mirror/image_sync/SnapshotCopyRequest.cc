// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SnapshotCopyRequest.h"
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
  send_snap_remove();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_remove() {
  CephContext *cct = m_local_image_ctx->cct;
  // TODO: issue #14937 needs to add support for cloned images
  {
    RWLock::RLocker snap_locker(m_remote_image_ctx->snap_lock);
    if (m_remote_image_ctx->parent_md.spec.pool_id != -1 ||
        std::find_if(m_remote_image_ctx->snap_info.begin(),
                     m_remote_image_ctx->snap_info.end(),
                     [](const std::pair<librados::snap_t, librbd::SnapInfo>& pair) {
            return pair.second.parent.spec.pool_id != -1;
          }) != m_remote_image_ctx->snap_info.end()) {
      lderr(cct) << "cloned images are not currentl supported" << dendl;
      finish(-EINVAL);
      return;
    }
  }

  librados::snap_t local_snap_id = CEPH_NOSNAP;
  while (local_snap_id == CEPH_NOSNAP && !m_local_snap_ids.empty()) {
    librados::snap_t snap_id = *m_local_snap_ids.begin();

    // if local snapshot id isn't in our mapping table, delete it
    // we match by id since snapshots can be renamed
    if (std::find_if(m_snap_seqs.begin(), m_snap_seqs.end(),
                     [snap_id](const SnapSeqs::value_type& pair) {
        return pair.second == snap_id; }) == m_snap_seqs.end()) {
      local_snap_id = snap_id;
      m_local_snap_ids.erase(m_local_snap_ids.begin());
    }
  }

  if (local_snap_id == CEPH_NOSNAP && m_local_snap_ids.empty()) {
    // no local snapshots to delete
    send_snap_create();
    return;
  }

  m_snap_name = get_snapshot_name(m_local_image_ctx, local_snap_id);

  ldout(cct, 20) << ": "
                 << "snap_name=" << m_snap_name << ", "
                 << "snap_id=" << local_snap_id << dendl;

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
    lderr(cct) << "failed to remove snapshot '" << m_snap_name << "': "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_snap_remove();
}

template <typename I>
void SnapshotCopyRequest<I>::send_snap_create() {
  librados::snap_t remote_snap_id = CEPH_NOSNAP;
  while (remote_snap_id == CEPH_NOSNAP && !m_remote_snap_ids.empty()) {
    librados::snap_t snap_id = *m_remote_snap_ids.begin();
    if (m_snap_seqs.find(snap_id) == m_snap_seqs.end()) {
      // missing remote -> local mapping
      remote_snap_id = snap_id;
    } else {
      // already have remote -> local mapping
      m_remote_snap_ids.erase(m_remote_snap_ids.begin());
    }
  }

  if (remote_snap_id == CEPH_NOSNAP && m_remote_snap_ids.empty()) {
    // no local snapshots to create
    send_update_client();
    return;
  }

  m_snap_name = get_snapshot_name(m_remote_image_ctx, remote_snap_id);

  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": "
                 << "snap_name=" << m_snap_name << ", "
                 << "snap_id=" << remote_snap_id << dendl;

  Context *ctx = create_context_callback<
    SnapshotCopyRequest<I>, &SnapshotCopyRequest<I>::handle_snap_create>(
      this);
  RWLock::RLocker owner_locker(m_local_image_ctx->owner_lock);
  m_local_image_ctx->operations->execute_snap_create(m_snap_name.c_str(), ctx,
                                                     0U);
}

template <typename I>
void SnapshotCopyRequest<I>::handle_snap_create(int r) {
  CephContext *cct = m_local_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to create snapshot '" << m_snap_name << "': "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  assert(!m_remote_snap_ids.empty());
  librados::snap_t remote_snap_id = *m_remote_snap_ids.begin();
  m_remote_snap_ids.erase(m_remote_snap_ids.begin());

  auto snap_it = m_local_image_ctx->snap_ids.find(m_snap_name);
  assert(snap_it != m_local_image_ctx->snap_ids.end());
  librados::snap_t local_snap_id = snap_it->second;

  ldout(cct, 20) << ": mapping remote snap id " << remote_snap_id << " to "
                 << local_snap_id << dendl;
  m_snap_seqs[remote_snap_id] = local_snap_id;

  send_snap_create();
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
    lderr(cct) << "failed to update client data: " << cpp_strerror(r) << dendl;
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
