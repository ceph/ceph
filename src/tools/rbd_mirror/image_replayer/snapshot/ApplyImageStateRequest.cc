// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ApplyImageStateRequest.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/image/GetMetadataRequest.h"
#include "tools/rbd_mirror/image_replayer/snapshot/Utils.h"
#include <boost/algorithm/string/predicate.hpp>

#include <shared_mutex> // for std::shared_lock

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::snapshot::" \
                           << "ApplyImageStateRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
ApplyImageStateRequest<I>::ApplyImageStateRequest(
    const std::string& local_mirror_uuid,
    const std::string& remote_mirror_uuid,
    I* local_image_ctx,
    I* remote_image_ctx,
    librbd::mirror::snapshot::ImageState image_state,
    Context* on_finish)
  : m_local_mirror_uuid(local_mirror_uuid),
    m_remote_mirror_uuid(remote_mirror_uuid),
    m_local_image_ctx(local_image_ctx),
    m_remote_image_ctx(remote_image_ctx),
    m_image_state(image_state),
    m_on_finish(on_finish) {
  dout(15) << "image_state=" << m_image_state << dendl;

  std::shared_lock image_locker{m_local_image_ctx->image_lock};
  m_features = m_local_image_ctx->features & ~RBD_FEATURES_IMPLICIT_ENABLE;
  compute_local_to_remote_snap_ids();
}

template <typename I>
void ApplyImageStateRequest<I>::send() {
  rename_image();
}

template <typename I>
void ApplyImageStateRequest<I>::rename_image() {
  std::shared_lock owner_locker{m_local_image_ctx->owner_lock};
  std::shared_lock image_locker{m_local_image_ctx->image_lock};
  if (m_local_image_ctx->name == m_image_state.name) {
    image_locker.unlock();
    owner_locker.unlock();

    update_features();
    return;
  }
  image_locker.unlock();

  dout(15) << "local_image_name=" << m_local_image_ctx->name << ", "
           << "remote_image_name=" << m_image_state.name << dendl;

  auto ctx = create_context_callback<
    ApplyImageStateRequest<I>,
    &ApplyImageStateRequest<I>::handle_rename_image>(this);
  m_local_image_ctx->operations->execute_rename(m_image_state.name, ctx);
}

template <typename I>
void ApplyImageStateRequest<I>::handle_rename_image(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to rename image to '" << m_image_state.name << "': "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  update_features();
}

template <typename I>
void ApplyImageStateRequest<I>::update_features() {
  uint64_t feature_updates = 0UL;
  bool enabled = false;

  auto image_state_features =
    m_image_state.features & ~RBD_FEATURES_IMPLICIT_ENABLE;
  feature_updates = (m_features & ~image_state_features);
  if (feature_updates == 0UL) {
    feature_updates = (image_state_features & ~m_features);
    enabled = (feature_updates != 0UL);
  }

  if (feature_updates == 0UL) {
    get_image_meta();
    return;
  }

  dout(15) << "image_features=" << m_features << ", "
           << "state_features=" << image_state_features << ", "
           << "feature_updates=" << feature_updates << ", "
           << "enabled=" << enabled << dendl;

  if (enabled) {
    m_features |= feature_updates;
  } else {
    m_features &= ~feature_updates;
  }

  std::shared_lock owner_lock{m_local_image_ctx->owner_lock};
  auto ctx = create_context_callback<
    ApplyImageStateRequest<I>,
    &ApplyImageStateRequest<I>::handle_update_features>(this);
  m_local_image_ctx->operations->execute_update_features(
    feature_updates, enabled, ctx, 0U);
}

template <typename I>
void ApplyImageStateRequest<I>::handle_update_features(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to update image features: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  update_features();
}

template <typename I>
void ApplyImageStateRequest<I>::get_image_meta() {
  dout(15) << dendl;

  auto ctx = create_context_callback<
    ApplyImageStateRequest<I>,
    &ApplyImageStateRequest<I>::handle_get_image_meta>(this);
  auto req = librbd::image::GetMetadataRequest<I>::create(
    m_local_image_ctx->md_ctx, m_local_image_ctx->header_oid, true, "", "", 0U,
    &m_metadata, ctx);
  req->send();
}

template <typename I>
void ApplyImageStateRequest<I>::handle_get_image_meta(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to fetch local image metadata: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  update_image_meta();
}

template <typename I>
void ApplyImageStateRequest<I>::update_image_meta() {
  std::set<std::string> keys_to_remove;
  for (const auto& [key, value] : m_metadata) {
    if (m_image_state.metadata.count(key) == 0) {
      dout(15) << "removing image-meta key '" << key << "'" << dendl;
      keys_to_remove.insert(key);
    }
  }

  std::map<std::string, bufferlist> metadata_to_update;
  for (const auto& [key, value] : m_image_state.metadata) {
    auto it = m_metadata.find(key);
    if (it == m_metadata.end() || !it->second.contents_equal(value)) {
      dout(15) << "updating image-meta key '" << key << "'" << dendl;
      metadata_to_update.insert({key, value});
    }
  }

  if (keys_to_remove.empty() && metadata_to_update.empty()) {
    unprotect_snapshot();
    return;
  }

  dout(15) << dendl;

  librados::ObjectWriteOperation op;
  for (const auto& key : keys_to_remove) {
    librbd::cls_client::metadata_remove(&op, key);
  }
  if (!metadata_to_update.empty()) {
    librbd::cls_client::metadata_set(&op, metadata_to_update);
  }

  auto aio_comp = create_rados_callback<
    ApplyImageStateRequest<I>,
    &ApplyImageStateRequest<I>::handle_update_image_meta>(this);
  int r = m_local_image_ctx->md_ctx.aio_operate(m_local_image_ctx->header_oid, aio_comp,
                                          &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ApplyImageStateRequest<I>::handle_update_image_meta(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to update image metadata: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_metadata.clear();

  m_prev_snap_id = CEPH_NOSNAP;
  unprotect_snapshot();
}

template <typename I>
void ApplyImageStateRequest<I>::unprotect_snapshot() {
  std::shared_lock image_locker{m_local_image_ctx->image_lock};

  auto snap_it = m_local_image_ctx->snap_info.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_it = m_local_image_ctx->snap_info.upper_bound(m_prev_snap_id);
  }

  for (; snap_it != m_local_image_ctx->snap_info.end(); ++snap_it) {
    auto snap_id = snap_it->first;
    const auto& snap_info = snap_it->second;

    auto user_ns = std::get_if<cls::rbd::UserSnapshotNamespace>(
      &snap_info.snap_namespace);
    if (user_ns == nullptr) {
      dout(20) << "snapshot " << snap_id << " is not a user snapshot" << dendl;
      continue;
    }

    if (snap_info.protection_status == RBD_PROTECTION_STATUS_UNPROTECTED) {
      dout(20) << "snapshot " << snap_id << " is already unprotected" << dendl;
      continue;
    }

    auto snap_id_map_it = m_local_to_remote_snap_ids.find(snap_id);
    if (snap_id_map_it == m_local_to_remote_snap_ids.end()) {
      dout(15) << "snapshot " << snap_id << " does not exist in remote image"
               << dendl;
      break;
    }

    auto remote_snap_id = snap_id_map_it->second;
    auto snap_state_it = m_image_state.snapshots.find(remote_snap_id);
    if (snap_state_it == m_image_state.snapshots.end()) {
      dout(15) << "snapshot " << snap_id << " does not exist in remote image "
               << "state" << dendl;
      break;
    }

    const auto& snap_state = snap_state_it->second;
    if (snap_state.protection_status == RBD_PROTECTION_STATUS_UNPROTECTED) {
      dout(15) << "snapshot " << snap_id << " is unprotected in remote image"
               << dendl;
      break;
    }
  }

  if (snap_it == m_local_image_ctx->snap_info.end()) {
    image_locker.unlock();

    // no local snapshots to unprotect
    m_prev_snap_id = CEPH_NOSNAP;
    remove_snapshot();
    return;
  }

  m_prev_snap_id = snap_it->first;
  m_snap_name = snap_it->second.name;
  image_locker.unlock();

  dout(15) << "snap_name=" << m_snap_name << ", "
           << "snap_id=" << m_prev_snap_id << dendl;

  std::shared_lock owner_locker{m_local_image_ctx->owner_lock};
  auto ctx = create_context_callback<
    ApplyImageStateRequest<I>,
    &ApplyImageStateRequest<I>::handle_unprotect_snapshot>(this);
  m_local_image_ctx->operations->execute_snap_unprotect(
    cls::rbd::UserSnapshotNamespace{}, m_snap_name.c_str(), ctx);
}

template <typename I>
void ApplyImageStateRequest<I>::handle_unprotect_snapshot(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to unprotect snapshot " << m_snap_name << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  unprotect_snapshot();
}

template <typename I>
void ApplyImageStateRequest<I>::remove_snapshot() {
  std::shared_lock image_locker{m_local_image_ctx->image_lock};

  auto snap_it = m_local_image_ctx->snap_info.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_it = m_local_image_ctx->snap_info.upper_bound(m_prev_snap_id);
  }

  for (; snap_it != m_local_image_ctx->snap_info.end(); ++snap_it) {
    auto snap_id = snap_it->first;
    const auto& snap_info = snap_it->second;

    auto user_ns = std::get_if<cls::rbd::UserSnapshotNamespace>(
      &snap_info.snap_namespace);
    if (user_ns == nullptr) {
      dout(20) << "snapshot " << snap_id << " is not a user snapshot" << dendl;
      continue;
    }

    auto snap_id_map_it = m_local_to_remote_snap_ids.find(snap_id);
    if (snap_id_map_it == m_local_to_remote_snap_ids.end()) {
      dout(15) << "snapshot " << snap_id << " does not exist in remote image"
               << dendl;
      break;
    }

    auto remote_snap_id = snap_id_map_it->second;
    auto snap_state_it = m_image_state.snapshots.find(remote_snap_id);
    if (snap_state_it == m_image_state.snapshots.end()) {
      dout(15) << "snapshot " << snap_id << " does not exist in remote image "
               << "state" << dendl;
      break;
    }
  }

  if (snap_it == m_local_image_ctx->snap_info.end()) {
    image_locker.unlock();

    // no local snapshots to remove
    m_prev_snap_id = CEPH_NOSNAP;
    protect_snapshot();
    return;
  }

  m_prev_snap_id = snap_it->first;
  m_snap_name = snap_it->second.name;
  image_locker.unlock();

  dout(15) << "snap_name=" << m_snap_name << ", "
           << "snap_id=" << m_prev_snap_id << dendl;

  std::shared_lock owner_locker{m_local_image_ctx->owner_lock};
  auto ctx = create_context_callback<
    ApplyImageStateRequest<I>,
    &ApplyImageStateRequest<I>::handle_remove_snapshot>(this);
  m_local_image_ctx->operations->execute_snap_remove(
    cls::rbd::UserSnapshotNamespace{}, m_snap_name.c_str(), ctx);
}

template <typename I>
void ApplyImageStateRequest<I>::handle_remove_snapshot(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to remove snapshot " << m_snap_name << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_snapshot();
}

template <typename I>
void ApplyImageStateRequest<I>::protect_snapshot() {
  std::shared_lock image_locker{m_local_image_ctx->image_lock};

  auto snap_it = m_local_image_ctx->snap_info.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_it = m_local_image_ctx->snap_info.upper_bound(m_prev_snap_id);
  }

  for (; snap_it != m_local_image_ctx->snap_info.end(); ++snap_it) {
    auto snap_id = snap_it->first;
    const auto& snap_info = snap_it->second;

    auto user_ns = std::get_if<cls::rbd::UserSnapshotNamespace>(
      &snap_info.snap_namespace);
    if (user_ns == nullptr) {
      dout(20) << "snapshot " << snap_id << " is not a user snapshot" << dendl;
      continue;
    }

    if (snap_info.protection_status == RBD_PROTECTION_STATUS_PROTECTED) {
      dout(20) << "snapshot " << snap_id << " is already protected" << dendl;
      continue;
    }

    auto snap_id_map_it = m_local_to_remote_snap_ids.find(snap_id);
    if (snap_id_map_it == m_local_to_remote_snap_ids.end()) {
      dout(15) << "snapshot " << snap_id << " does not exist in remote image"
               << dendl;
      continue;
    }

    auto remote_snap_id = snap_id_map_it->second;
    auto snap_state_it = m_image_state.snapshots.find(remote_snap_id);
    if (snap_state_it == m_image_state.snapshots.end()) {
      dout(15) << "snapshot " << snap_id << " does not exist in remote image "
               << "state" << dendl;
      continue;
    }

    const auto& snap_state = snap_state_it->second;
    if (snap_state.protection_status == RBD_PROTECTION_STATUS_PROTECTED) {
      dout(15) << "snapshot " << snap_id << " is protected in remote image"
               << dendl;
      break;
    }
  }

  if (snap_it == m_local_image_ctx->snap_info.end()) {
    image_locker.unlock();

    // no local snapshots to protect
    m_prev_snap_id = CEPH_NOSNAP;
    rename_snapshot();
    return;
  }

  m_prev_snap_id = snap_it->first;
  m_snap_name = snap_it->second.name;
  image_locker.unlock();

  dout(15) << "snap_name=" << m_snap_name << ", "
           << "snap_id=" << m_prev_snap_id << dendl;

  std::shared_lock owner_locker{m_local_image_ctx->owner_lock};
  auto ctx = create_context_callback<
    ApplyImageStateRequest<I>,
    &ApplyImageStateRequest<I>::handle_protect_snapshot>(this);
  m_local_image_ctx->operations->execute_snap_protect(
    cls::rbd::UserSnapshotNamespace{}, m_snap_name.c_str(), ctx);
}

template <typename I>
void ApplyImageStateRequest<I>::handle_protect_snapshot(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to protect snapshot " << m_snap_name << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  protect_snapshot();
}

template <typename I>
void ApplyImageStateRequest<I>::rename_snapshot() {
  std::shared_lock image_locker{m_local_image_ctx->image_lock};

  auto snap_it = m_local_image_ctx->snap_info.begin();
  if (m_prev_snap_id != CEPH_NOSNAP) {
    snap_it = m_local_image_ctx->snap_info.upper_bound(m_prev_snap_id);
  }

  for (; snap_it != m_local_image_ctx->snap_info.end(); ++snap_it) {
    auto snap_id = snap_it->first;
    const auto& snap_info = snap_it->second;

    auto user_ns = std::get_if<cls::rbd::UserSnapshotNamespace>(
      &snap_info.snap_namespace);
    if (user_ns == nullptr) {
      dout(20) << "snapshot " << snap_id << " is not a user snapshot" << dendl;
      continue;
    }

    auto snap_id_map_it = m_local_to_remote_snap_ids.find(snap_id);
    if (snap_id_map_it == m_local_to_remote_snap_ids.end()) {
      dout(15) << "snapshot " << snap_id << " does not exist in remote image"
               << dendl;
      continue;
    }

    auto remote_snap_id = snap_id_map_it->second;
    auto snap_state_it = m_image_state.snapshots.find(remote_snap_id);
    if (snap_state_it == m_image_state.snapshots.end()) {
      dout(15) << "snapshot " << snap_id << " does not exist in remote image "
               << "state" << dendl;
      continue;
    }

    const auto& snap_state = snap_state_it->second;
    if (snap_info.name != snap_state.name) {
      dout(15) << "snapshot " << snap_id << " has been renamed from '"
               << snap_info.name << "' to '" << snap_state.name << "'"
               << dendl;
      m_snap_name = snap_state.name;
      break;
    }
  }

  if (snap_it == m_local_image_ctx->snap_info.end()) {
    image_locker.unlock();

    // no local snapshots to protect
    m_prev_snap_id = CEPH_NOSNAP;
    set_snapshot_limit();
    return;
  }

  m_prev_snap_id = snap_it->first;
  image_locker.unlock();

  dout(15) << "snap_name=" << m_snap_name << ", "
           << "snap_id=" << m_prev_snap_id << dendl;

  std::shared_lock owner_locker{m_local_image_ctx->owner_lock};
  auto ctx = create_context_callback<
    ApplyImageStateRequest<I>,
    &ApplyImageStateRequest<I>::handle_rename_snapshot>(this);
  m_local_image_ctx->operations->execute_snap_rename(
    m_prev_snap_id, m_snap_name.c_str(), ctx);
}

template <typename I>
void ApplyImageStateRequest<I>::handle_rename_snapshot(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to protect snapshot " << m_snap_name << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  rename_snapshot();
}

template <typename I>
void ApplyImageStateRequest<I>::set_snapshot_limit() {
  dout(15) << "snap_limit=" << m_image_state.snap_limit << dendl;

  // no need to even check the current limit -- just set it
  std::shared_lock owner_locker{m_local_image_ctx->owner_lock};
  auto ctx = create_context_callback<
    ApplyImageStateRequest<I>,
    &ApplyImageStateRequest<I>::handle_set_snapshot_limit>(this);
  m_local_image_ctx->operations->execute_snap_set_limit(
    m_image_state.snap_limit, ctx);
}

template <typename I>
void ApplyImageStateRequest<I>::handle_set_snapshot_limit(int r) {
  dout(15) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to update snapshot limit: " << cpp_strerror(r)
         << dendl;
  }

  finish(r);
}

template <typename I>
void ApplyImageStateRequest<I>::finish(int r) {
  dout(15) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

template <typename I>
uint64_t ApplyImageStateRequest<I>::compute_remote_snap_id(
    uint64_t local_snap_id) {
  ceph_assert(ceph_mutex_is_locked(m_local_image_ctx->image_lock));
  ceph_assert(ceph_mutex_is_locked(m_remote_image_ctx->image_lock));

  // Search our local non-primary snapshots for a mapping to the remote
  // snapshot. The non-primary mirror snapshot with the mappings will always
  // come at or after the snapshot we are searching against
  auto remote_snap_id = util::compute_remote_snap_id(
    m_local_image_ctx->image_lock, m_local_image_ctx->snap_info,
    local_snap_id, m_remote_mirror_uuid);
  if (remote_snap_id != CEPH_NOSNAP) {
    return remote_snap_id;
  }

  // if we failed to find a match to a remote snapshot in our local non-primary
  // snapshots, check the remote image for non-primary snapshot mappings back
  // to our snapshot
  for (auto snap_it = m_remote_image_ctx->snap_info.begin();
       snap_it != m_remote_image_ctx->snap_info.end(); ++snap_it) {
    auto snap_id = snap_it->first;
    auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
      &snap_it->second.snap_namespace);
    if (mirror_ns == nullptr || !mirror_ns->is_non_primary()) {
      continue;
    }

    if (mirror_ns->primary_mirror_uuid != m_local_mirror_uuid) {
      dout(20) << "remote snapshot " << snap_id << " not tied to local"
               << dendl;
      continue;
    } else if (mirror_ns->primary_snap_id == local_snap_id) {
        dout(15) << "local snapshot " << local_snap_id << " maps to "
                 << "remote snapshot " << snap_id << dendl;
        return snap_id;
    }

    const auto& snap_seqs = mirror_ns->snap_seqs;
    for (auto [local_snap_id_seq, remote_snap_id_seq] : snap_seqs) {
      if (local_snap_id_seq == local_snap_id) {
        dout(15) << "local snapshot " << local_snap_id << " maps to "
                 << "remote snapshot " << remote_snap_id_seq << dendl;
        return remote_snap_id_seq;
      }
    }
  }

  return CEPH_NOSNAP;
}

template <typename I>
void ApplyImageStateRequest<I>::compute_local_to_remote_snap_ids() {
  ceph_assert(ceph_mutex_is_locked(m_local_image_ctx->image_lock));
  std::shared_lock remote_image_locker{m_remote_image_ctx->image_lock};

  for (const auto& [snap_id, snap_info] : m_local_image_ctx->snap_info) {
    m_local_to_remote_snap_ids[snap_id] = compute_remote_snap_id(snap_id);
  }

  dout(15) << "local_to_remote_snap_ids=" << m_local_to_remote_snap_ids
           << dendl;
}

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::snapshot::ApplyImageStateRequest<librbd::ImageCtx>;
