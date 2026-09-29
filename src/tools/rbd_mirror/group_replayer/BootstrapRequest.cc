// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/compat.h"
#include "BootstrapRequest.h"
#include "CreateLocalGroupRequest.h"
#include "GroupStateBuilder.h"
#include "PrepareLocalGroupRequest.h"
#include "PrepareRemoteGroupRequest.h"
#include "RemoveLocalGroupRequest.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/internal.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/group/RemoveImageRequest.h"
#include "librbd/group/AddImageRequest.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_deleter/TrashMoveRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::" \
                           << "BootstrapRequest: " << this << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
BootstrapRequest<I>::BootstrapRequest(
    Threads<I> *threads,
    librados::IoCtx &local_io_ctx,
    librados::IoCtx &remote_io_ctx,
    const std::string &global_group_id,
    const std::string &local_mirror_uuid,
    InstanceWatcher<I> *instance_watcher,
    MirrorStatusUpdater<I> *local_status_updater,
    MirrorStatusUpdater<I> *remote_status_updater,
    journal::CacheManagerHandler *cache_manager_handler,
    PoolMetaCache *pool_meta_cache,
    bool *resync_requested,
    GroupCtx *local_group_ctx,
    std::list<std::pair<librados::IoCtx, ImageReplayer<I> *>> *image_replayers,
    GroupStateBuilder<I> **state_builder,
    Context* on_finish)
  : CancelableRequest("rbd::mirror::group_replayer::BootstrapRequest",
		      reinterpret_cast<CephContext*>(local_io_ctx.cct()),
                      on_finish),
    m_threads(threads),
    m_local_io_ctx(local_io_ctx),
    m_remote_io_ctx(remote_io_ctx),
    m_global_group_id(global_group_id),
    m_local_mirror_uuid(local_mirror_uuid),
    m_instance_watcher(instance_watcher),
    m_local_status_updater(local_status_updater),
    m_remote_status_updater(remote_status_updater),
    m_cache_manager_handler(cache_manager_handler),
    m_pool_meta_cache(pool_meta_cache),
    m_resync_requested(resync_requested),
    m_local_group_ctx(local_group_ctx),
    m_image_replayers(image_replayers),
    m_state_builder(state_builder),
    m_on_finish(on_finish),
    m_lock(ceph::make_mutex(librbd::util::unique_lock_name(
        "BootstrapRequest::m_lock", this))){
  dout(10)  << "global_group_id=" << m_global_group_id << dendl;
}

template <typename I>
int BootstrapRequest<I>::get_local_image_id(const std::string &global_image_id,
  std::string *image_id) {
  return librbd::cls_client::mirror_image_get_image_id(&m_local_io_ctx,
    global_image_id, image_id);
}

template <typename I>
void BootstrapRequest<I>::send() {
  ceph_assert(*m_state_builder == nullptr);
// TODO : Create this in PrepareLocalGroupRequest/PrepareRemoteGroupRequest ?
  *m_state_builder = GroupStateBuilder<I>::create(m_global_group_id);

  prepare_local_group();
}

template <typename I>
void BootstrapRequest<I>::cancel() {
  dout(10) << dendl;

  m_canceled = true;
}

template <typename I>
void BootstrapRequest<I>::prepare_local_group() {
  dout(10) << dendl;

  m_local_group_removed = false;
  auto ctx = create_context_callback<
    BootstrapRequest, &BootstrapRequest<I>::handle_prepare_local_group>(this);
  auto req = PrepareLocalGroupRequest<I>::create(
    m_local_io_ctx, m_global_group_id, &m_prepare_local_group_name,
    m_state_builder, m_threads->work_queue, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_prepare_local_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << "local group does not exist" << dendl;
  } else if (r < 0) {
    derr << "error preparing local group: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  if (!m_prepare_local_group_name.empty()) {
    std::lock_guard locker{m_lock};
    m_local_group_name = m_prepare_local_group_name;
  }

  prepare_remote_group();
}

template <typename I>
void BootstrapRequest<I>::prepare_remote_group() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    BootstrapRequest, &BootstrapRequest<I>::handle_prepare_remote_group>(this);
  auto req = PrepareRemoteGroupRequest<I>::create(
    m_remote_io_ctx, m_global_group_id, &m_prepare_remote_group_name,
    m_state_builder, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_prepare_remote_group(int r) {
  dout(10) << "r=" << r << dendl;

  auto state_builder = *m_state_builder;

  if (state_builder->is_local_primary()) {
    dout(5) << "local group is primary" << dendl;
    finish(0);
    return;
  }

  if (!state_builder->local_group_id.empty() &&
      state_builder->local_mirror_group.state ==
        cls::rbd::MIRROR_GROUP_STATE_DISABLING) {
    // Removal is multi-step and can be interrupted after persisting the
    // DISABLING state. Always finish tearing down a non-primary local group,
    // if the remote group is still enabled, the next bootstrap will recreate
    // the secondary from it.
    dout(10) << "resuming interrupted local group removal" << dendl;
    remove_local_group();
    return;
  }

  if (r == -ENOENT) {
    if (state_builder->remote_group_id.empty()) {
      if (state_builder->local_group_id.empty()) {
        // Neither group exists
        // Tell the caller to discard the replayer for this missing group.
        m_local_group_removed = true;
        finish(0);
        return;
      } else if (state_builder->local_mirror_group.state ==
                   cls::rbd::MIRROR_GROUP_STATE_CREATING ||
                 state_builder->local_promotion_state ==
                   librbd::mirror::PROMOTION_STATE_NON_PRIMARY) {
        // CREATING is used only while constructing a local secondary. An
        // empty group has no mirror snapshot from which GroupGetInfoRequest
        // can infer NON_PRIMARY, so a rapid remote disable can otherwise
        // strand the local group with an UNKNOWN promotion state.
        remove_local_group();
        return;
      } else {
	// Do not remove the group if the promotion state is orphan or unknown
	finish(-ENOLINK);
	return;
      }
    }
  } else if (r < 0) {
    derr << "error preparing remote group: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  if (!state_builder->is_remote_primary()) {
    if (state_builder->local_group_id.empty()) {
      // local group does not exist and remote is not primary
      dout(10) << "local group does not exist and remote group is not primary"
               << dendl;
      finish(-EREMOTEIO);
      return;
    } else if (!state_builder->is_linked()) {
      dout(10) << "local group is not non-primary and remote group is not primary"
               << dendl;
      finish(-EREMOTEIO);
      return;
    }
  }

  if (state_builder->local_group_id.empty()) {
    // create the local group
    create_local_group();
    return;
  } else {
    // Local group is secondary.
    if (m_local_group_name != (*m_state_builder)->group_name) {
      finish(-EREMCHG);
      return;
    }
    // See if resync is set.
    get_local_group_meta();
  }
}

template <typename I>
void BootstrapRequest<I>::create_local_group() {
  dout(10) << dendl;
  auto ctx = create_context_callback<
    BootstrapRequest, &BootstrapRequest<I>::handle_create_local_group>(this);
  auto req = CreateLocalGroupRequest<I>::create(
    m_local_io_ctx, m_global_group_id, *m_state_builder, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error creating local group: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }
  finish(0);
}

template <typename I>
void BootstrapRequest<I>::remove_local_group() {
  dout(10) << dendl;

  // Processing each saved membership in order means that a GroupReplayer can
  // own local images which have not yet been added to the current local group
  // header. If the remote group disappears, RemoveLocalGroupRequest cannot
  // discover those images by listing the header alone. Include every such
  // image whose remote mirror identity has also disappeared so group disable
  // cannot leave stale named images behind.
  std::map<std::string, std::pair<int64_t, std::string>> extra_trash_images;
  if (*m_resync_requested) {
    // The local group header can be older than the remote primary after a
    // force promotion. Include every live remote member in the resync set so
    // that an image detached from the stale local header is also recreated.
    // Reusing that standalone local image would retain its divergent mirror
    // snapshot history and immediately report split-brain after reattachment.
    for (const auto &[global_image_id, remote_image] :
      (*m_state_builder)->remote_images) {
      std::string local_image_id;
      int r = librbd::cls_client::mirror_image_get_image_id(&m_local_io_ctx,
        global_image_id, &local_image_id);
      if (r == 0) {
        extra_trash_images.emplace(global_image_id,
          std::make_pair(m_local_io_ctx.get_id(), std::move(local_image_id)));
      } else if (r != -ENOENT) {
        derr << "failed to resolve local image " << global_image_id << ": "
             << cpp_strerror(r) << dendl;
      }
    }
  }

  for (auto &[local_io_ctx, image_replayer] : *m_image_replayers) {
    const auto &global_image_id = image_replayer->get_global_image_id();

    std::string remote_image_id;
    int r = librbd::cls_client::mirror_image_get_image_id(&m_remote_io_ctx,
      global_image_id, &remote_image_id);
    if (r == 0) {
      // The image was detached from the group but remains mirrored as a
      // standalone image. Its standalone ImageReplayer owns its lifecycle.
      continue;
    } else if (r != -ENOENT) {
      derr << "failed to check remote image " << global_image_id << ": "
           << cpp_strerror(r) << dendl;
      continue;
    }

    std::string local_image_id;
    r = librbd::cls_client::mirror_image_get_image_id(&local_io_ctx,
      global_image_id, &local_image_id);
    if (r == 0) {
      extra_trash_images.emplace(global_image_id,
        std::make_pair(local_io_ctx.get_id(), std::move(local_image_id)));
    } else if (r != -ENOENT) {
      derr << "failed to resolve local image " << global_image_id << ": "
           << cpp_strerror(r) << dendl;
    }
  }

  dout(10) << "extra images to trash=" << extra_trash_images << dendl;

  auto ctx = create_context_callback<
    BootstrapRequest,
    &BootstrapRequest<I>::handle_remove_local_group>(this);

  auto req = RemoveLocalGroupRequest<I>::create(
    m_local_io_ctx, m_global_group_id, (*m_state_builder)->local_group_id,
    (*m_state_builder)->local_mirror_group, *m_resync_requested,
    m_threads->work_queue, extra_trash_images, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_remove_local_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error removing local group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }
  m_local_group_removed = true;
  finish(0);
}

template <typename I>
void BootstrapRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    r = -ECANCELED;
    m_on_finish->complete(r);
    return;
  }

  if (r == 0) {
    if (m_local_group_removed) {
      r = -ENOENT;
    } else {
      *m_local_group_ctx = {(*m_state_builder)->group_name,
                            (*m_state_builder)->local_group_id,
                            m_global_group_id,
                            (*m_state_builder)->is_local_primary(),
                             m_local_io_ctx};

      if ((*m_state_builder)->is_local_primary()) {
        handle_reconcile_local_group_members(0);
        return;
      }
      if ((*m_state_builder)->local_group_id.empty()) {
        handle_reconcile_local_group_members(0);
        return;
      }
      load_local_group_snapshots();
      return;
    }
  }

  m_on_finish->complete(r);
}

template <typename I>
void BootstrapRequest<I>::load_local_group_snapshots() {
  m_local_group_snaps.clear();
  auto ctx = create_context_callback<BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_load_local_group_snapshots>(this);
  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_local_io_ctx,
    (*m_state_builder)->local_group_id, true, true, &m_local_group_snaps, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_load_local_group_snapshots(int r) {
  if (r < 0) {
    derr << "failed to load local group snapshots: " << cpp_strerror(r)
         << dendl;
    m_on_finish->complete(r);
    return;
  }
  load_remote_group_snapshots();
}

template <typename I>
void BootstrapRequest<I>::load_remote_group_snapshots() {
  m_remote_group_snaps.clear();
  auto ctx = create_context_callback<BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_load_remote_group_snapshots>(this);
  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_remote_io_ctx,
    (*m_state_builder)->remote_group_id, true, true, &m_remote_group_snaps,
    ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_load_remote_group_snapshots(int r) {
  if (r < 0) {
    derr << "failed to load remote group snapshots: " << cpp_strerror(r)
         << dendl;
    m_on_finish->complete(r);
    return;
  }

  r = select_target_group_membership();
  if (r < 0) {
    if (r == -EEXIST) {
      // Preserve image status for a split-brain group. Membership must not be
      // reconciled, but the GroupReplayer still needs an ImageReplayer for
      // each remote member so the group status includes its images.
      int create_r = create_replayers();
      if (create_r < 0) {
        r = create_r;
      }
    }
    m_on_finish->complete(r);
    return;
  }
  reconcile_local_group_members();
}

template <typename I>
int BootstrapRequest<I>::select_target_group_membership() {
  auto state_builder = *m_state_builder;
  m_target_remote_images = state_builder->remote_images;

  std::string last_local_snap_id;
  for (auto it = m_local_group_snaps.rbegin(); it != m_local_group_snaps.rend();
    ++it) {
    if (it->state != cls::rbd::GROUP_SNAPSHOT_STATE_CREATED) {
      continue;
    }

    auto mirror_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &it->snapshot_namespace);
    if (mirror_ns != nullptr &&
        !is_mirror_group_snapshot_complete(it->state, mirror_ns->complete)) {
      continue;
    }

    last_local_snap_id = it->id;
    break;
  }

  bool found_last = last_local_snap_id.empty();
  const cls::rbd::GroupSnapshot *target_snap = nullptr;
  for (const auto &snap : m_remote_group_snaps) {
    if (!found_last) {
      if (snap.id == last_local_snap_id) {
        found_last = true;
      }
      continue;
    }

    auto mirror_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &snap.snapshot_namespace);
    if (mirror_ns == nullptr || !mirror_ns->is_primary() ||
        !is_mirror_group_snapshot_complete(snap.state, mirror_ns->complete)) {
      continue;
    }
    target_snap = &snap;
    break;
  }

  if (!last_local_snap_id.empty() && !found_last) {
    derr << "latest local group snapshot is absent from remote: "
         << last_local_snap_id << dendl;
    return -EEXIST;
  }

  if (target_snap == nullptr) {
    return 0;
  }

  int r = check_image_handoff_order(*target_snap);
  if (r < 0) {
    return r;
  }

  m_target_remote_images.clear();
  for (const auto &spec : target_snap->snaps) {
    cls::rbd::MirrorImage mirror_image;
    r = librbd::cls_client::mirror_image_get(&m_remote_io_ctx,
      spec.image_id, &mirror_image);
    if (r == -ENOENT) {
      // A membership snapshot can outlive an image that was subsequently
      // detached and deleted on the primary. The missing image is no longer
      // available, but it does not mean that the group itself was removed.
      // Exclude it from the target membership so bootstrap can advance to the
      // later detach/empty membership snapshots.
      dout(10) << "remote image " << spec.image_id
               << " from target snapshot no longer exists" << dendl;
      continue;
    }
    if (r < 0) {
      derr << "failed to load remote mirror image " << spec.image_id << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
    m_target_remote_images.emplace(mirror_image.global_image_id,
      std::make_pair(spec.pool, spec.image_id));
  }

  dout(10) << "target snapshot=" << target_snap->id
           << ", target images=" << m_target_remote_images << dendl;
  return 0;
}

template <typename I>
int BootstrapRequest<I>::check_image_handoff_order(
    const cls::rbd::GroupSnapshot& target_snap) const {
  auto state_builder = *m_state_builder;

  for (const auto& spec : target_snap.snaps) {
    if (spec.snap_id == CEPH_NOSNAP) {
      continue;
    }

    auto image_header_oid = librbd::util::header_name(spec.image_id);
    ::SnapContext snapc;
    int r = librbd::cls_client::get_snapcontext(&m_remote_io_ctx,
      image_header_oid, &snapc);
    if (r == -ENOENT) {
      continue;
    } else if (r < 0) {
      derr << "failed to list snapshots for remote image " << spec.image_id
           << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    for (auto snap_id : snapc.snaps) {
      if (snap_id >= spec.snap_id) {
        continue;
      }

      cls::rbd::SnapshotInfo snap_info;
      r = librbd::cls_client::snapshot_get(&m_remote_io_ctx,
        image_header_oid, snap_id, &snap_info);
      if (r == -ENOENT) {
        continue;
      } else if (r < 0) {
        derr << "failed to load snapshot " << snap_id << " for remote image "
             << spec.image_id << ": " << cpp_strerror(r) << dendl;
        return r;
      }

      auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
        &snap_info.snapshot_namespace);
      if (mirror_ns == nullptr || !mirror_ns->is_primary() ||
          mirror_ns->group_snap_id.empty() ||
          mirror_ns->mirror_peer_uuids.count(m_local_mirror_uuid) == 0) {
        continue;
      }

      if (mirror_ns->group_spec.pool_id == m_remote_io_ctx.get_id() &&
          mirror_ns->group_spec.group_id == state_builder->remote_group_id) {
        continue;
      }

      // The earlier group must finish with the image before this group can
      // claim it. Otherwise either group can win after the old owner releases
      // the image, leaving the other group to retry forever.
      dout(10) << "waiting for image " << spec.image_id
               << " snapshot " << snap_id << " from group "
               << mirror_ns->group_spec << dendl;
      return -EAGAIN;
    }
  }

  return 0;
}

template <typename I>
void BootstrapRequest<I>::reconcile_local_group_members() {
  auto state_builder = *m_state_builder;
  dout(10) << "local images=" << state_builder->local_images
           << ", local images without mirror metadata="
           << state_builder->local_images_without_mirror_metadata.size()
           << ", target remote images=" << m_target_remote_images << dendl;

  // A local primary owns its membership and must not follow the remote group.
  if (state_builder->is_local_primary()) {
    handle_reconcile_local_group_members(0);
    return;
  }

  m_remove_group_images.clear();
  m_add_group_images.clear();

  // An image can be deleted on the primary after it is detached while the
  // secondary still has the old group membership. In that case its local
  // mirror metadata can disappear before this replayer processes the detach
  // snapshot. The image cannot be matched by global id, but it is still a
  // stale local member and must be detached before the replayer continues.
  m_remove_group_images.insert(m_remove_group_images.end(),
    state_builder->local_images_without_mirror_metadata.begin(),
    state_builder->local_images_without_mirror_metadata.end());

  // Remove local members that are no longer present remotely.
  for (const auto &[global_image_id, local] : state_builder->local_images) {

    if (m_target_remote_images.count(global_image_id) == 0) {

      cls::rbd::GroupImageSpec spec;
      spec.pool_id = local.first;
      spec.image_id = local.second;

      m_remove_group_images.push_back(spec);
    }
  }

  // Add remote members missing locally.
  for (const auto &[global_image_id, remote] : m_target_remote_images) {

    // Already a member locally.
    if (state_builder->local_images.count(global_image_id) != 0) {
      continue;
    }

    cls::rbd::GroupImageSpec spec;
    spec.pool_id = m_local_io_ctx.get_id();

    int r = get_local_image_id(global_image_id, &spec.image_id);
    if (r == -ENOENT) {
      dout(10) << "local image for global_image_id=" << global_image_id
               << " not found yet" << dendl;
      continue;
    } else if (r < 0) {
      derr << "failed to resolve local image for global_image_id="
           << global_image_id << ": " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    m_add_group_images.push_back(spec);
  }

  if (!m_remove_group_images.empty()) {
    remove_next_group_member();
    return;
  }

  if (!m_add_group_images.empty()) {
    add_next_group_member();
    return;
  }

  handle_reconcile_local_group_members(0);
}

template <typename I>
void BootstrapRequest<I>::remove_next_group_member() {

  auto spec = m_remove_group_images.front();
  m_remove_group_images.pop_front();

  dout(10) << "removing image_id=" << spec.image_id << " from local group "
           << (*m_state_builder)->local_group_id << dendl;

  auto ctx = create_context_callback<BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_remove_group_member>(this);

  auto req = librbd::group::RemoveImageRequest<I>::create(m_local_io_ctx,
    (*m_state_builder)->local_group_id, m_local_io_ctx, spec.image_id, ctx);

  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_remove_group_member(int r) {

  if (r < 0 && r != -ENOENT) {
    derr << "failed removing stale group member: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_remove_group_images.empty()) {
    remove_next_group_member();
    return;
  }

  if (!m_add_group_images.empty()) {
    add_next_group_member();
    return;
  }

  handle_reconcile_local_group_members(0);
}

template <typename I>
void BootstrapRequest<I>::add_next_group_member() {

  auto spec = m_add_group_images.front();
  m_add_group_images.pop_front();

  dout(10) << "adding image_id=" << spec.image_id << " to local group "
           << (*m_state_builder)->local_group_id << dendl;

  auto ctx = create_context_callback<BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_add_group_member>(this);

  // Use the mirroring-aware request here.
  auto req = librbd::group::AddImageRequest<I>::create(m_local_io_ctx,
    (*m_state_builder)->local_group_id, m_local_io_ctx, spec.image_id, ctx);

  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_add_group_member(int r) {

  if (r == -EEXIST) {
    // Another group can legitimately own this image while it finishes an
    // older membership snapshot. Retry bootstrap after that group detaches
    // the image instead of reporting the transient ownership conflict as
    // split-brain.
    r = -EAGAIN;
  }

  if (r < 0 && r != -ENOENT) {
    derr << "failed adding missing group member: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_add_group_images.empty()) {
    add_next_group_member();
    return;
  }

  handle_reconcile_local_group_members(0);
}

template <typename I>
void BootstrapRequest<I>::handle_reconcile_local_group_members(int r) {

  if (r < 0) {
    finish(r);
    return;
  }

  r = create_replayers();
  if (r < 0) {
    m_on_finish->complete(r);
    return;
  }

  m_on_finish->complete(0);
}

template <typename I>
int BootstrapRequest<I>::create_replayers() {
  dout(10) << dendl;

  std::string remote_fsid;
  librados::Rados remote_rados(m_remote_io_ctx);
  int r = remote_rados.cluster_fsid(&remote_fsid);
  if (r < 0) {
    derr << "failed to retrieve remote cluster fsid: " << cpp_strerror(r)
	 << dendl;
    return r;
  }

  auto state_builder = *m_state_builder;

  auto owned_remote_images = m_target_remote_images;
  if (!state_builder->is_local_primary()) {
    for (auto it = owned_remote_images.begin();
      it != owned_remote_images.end();) {
      cls::rbd::MirrorImage mirror_image;
      r = librbd::cls_client::mirror_image_get(&m_remote_io_ctx,
        it->second.second, &mirror_image);
      if (r < 0) {
        derr << "failed to load remote mirror image " << it->second.second
             << ": " << cpp_strerror(r) << dendl;
        return r;
      }

      // A detached image can remain in a historical group snapshot while
      // being mirrored standalone. Its standalone ImageReplayer must retain
      // ownership, the group replayer only sets its snapshot limit.
      if (mirror_image.type == cls::rbd::MIRROR_IMAGE_TYPE_STANDALONE) {
        it = owned_remote_images.erase(it);
      } else {
        ++it;
      }
    }
  }

  std::set<std::string> desired_global_image_ids;
  if (state_builder->is_local_primary()) {
    for (const auto &[global_image_id, _] : state_builder->local_images) {
      desired_global_image_ids.insert(global_image_id);
    }
  } else if (!state_builder->remote_group_id.empty()) {
    for (const auto &[global_image_id, _] : owned_remote_images) {
      desired_global_image_ids.insert(global_image_id);
    }
  }

  // Remove replayers that are outside the selected membership.
  for (auto it = m_image_replayers->begin(); it != m_image_replayers->end();) {

    auto image_replayer = it->second;
    ceph_assert(image_replayer != nullptr);

    const auto &global_image_id = image_replayer->get_global_image_id();

    if (!desired_global_image_ids.contains(global_image_id)) {
      dout(10) << "removing stale image replayer for global image id: "
               << global_image_id << dendl;

      ceph_assert(image_replayer->is_stopped());
      image_replayer->destroy();
      it = m_image_replayers->erase(it);
      continue;
    }

    ++it;
  }

  if (state_builder->is_local_primary()) {
    // The ImageReplayers are required to run even when the group
    // is primary in order to update the image status for the
    // mirror pool status to be healthy.
    for (auto &[global_image_id, p] : state_builder->local_images) {
      auto &local_pool_id = p.first;
      bool is_image_replayer_exists = std::any_of(
        m_image_replayers->begin(), m_image_replayers->end(),
        [&global_image_id](const auto& entry) {
          return entry.second->get_global_image_id() == global_image_id;
        });
      if (is_image_replayer_exists) {
        dout(10) << "image replayer for global image id: " << global_image_id
                 << " already exists" << dendl;
        continue;
      }

      m_image_replayers->emplace_back(librados::IoCtx(), nullptr);
      auto &local_io_ctx = m_image_replayers->back().first;
      auto &image_replayer = m_image_replayers->back().second;

      LocalPoolMeta local_pool_meta;
      r = m_pool_meta_cache->get_local_pool_meta(local_pool_id,
                                                 &local_pool_meta);
      if (r < 0 || local_pool_meta.mirror_uuid.empty()) {
        if (r == 0 || r == -ENOENT) {
          r = -EINVAL;
        }
        derr << "failed to retrieve mirror uuid from local image pool" << dendl;
        break;
      }

      r = librbd::util::create_ioctx(m_local_io_ctx, "local image pool",
                                     local_pool_id, {}, &local_io_ctx);
      if (r < 0) {
        derr << "failed to open local image pool " << local_pool_id << ": "
             << cpp_strerror(r) << dendl;
        if (r == -ENOENT) {
          r = -EINVAL;
        }
        break;
      }

      int64_t remote_pool_id = remote_rados.pool_lookup(
          local_io_ctx.get_pool_name().c_str());

      RemotePoolMeta remote_pool_meta;
      r = m_pool_meta_cache->get_remote_pool_meta(remote_fsid, remote_pool_id,
                                                  &remote_pool_meta);
      if (r < 0 || remote_pool_meta.mirror_peer_uuid.empty()) {
        derr << "failed to retrieve mirror peer uuid from remote image pool"
             << dendl;
        r = -ENOENT;
        break;
      }

      librados::IoCtx remote_io_ctx;
      r = librbd::util::create_ioctx(m_remote_io_ctx, "remote image pool",
                                     remote_pool_id, {}, &remote_io_ctx);
      if (r < 0) {
        derr << "failed to open remote image pool " << remote_pool_id << ": "
             << cpp_strerror(r) << dendl;
        if (r == -ENOENT) {
          r = -EINVAL;
        }
        break;
      }

      image_replayer = ImageReplayer<I>::create(
        local_io_ctx, m_local_group_ctx, local_pool_meta.mirror_uuid,
        global_image_id, m_threads, m_instance_watcher, m_local_status_updater,
        m_cache_manager_handler, m_pool_meta_cache);

      // TODO only a single peer is currently supported
      image_replayer->add_peer({local_pool_meta.mirror_uuid, remote_io_ctx,
                                remote_pool_meta, m_remote_status_updater});
    }
  } else if (!state_builder->remote_group_id.empty()) {
    // Bootstrap reconciles the local group to one remote snapshot at a time.
    // Its replayers must match that same target membership: a historical
    // snapshot can contain an image which has since moved to another group,
    // and the old group must finish processing it before releasing ownership.
    for (auto &[global_image_id, p] : owned_remote_images) {
      auto &remote_pool_id = p.first;
      bool is_image_replayer_exists = std::any_of(
        m_image_replayers->begin(), m_image_replayers->end(),
        [&global_image_id](const auto& entry) {
          return entry.second->get_global_image_id() == global_image_id;
        });
      if (is_image_replayer_exists) {
        dout(10) << "image replayer for global image id: " << global_image_id
                 << " already exists" << dendl;
        continue;
      }

      m_image_replayers->emplace_back(librados::IoCtx(), nullptr);
      auto &local_io_ctx = m_image_replayers->back().first;
      auto &image_replayer = m_image_replayers->back().second;

      RemotePoolMeta remote_pool_meta;
      r = m_pool_meta_cache->get_remote_pool_meta(remote_fsid, remote_pool_id,
                                                  &remote_pool_meta);
      if (r < 0 || remote_pool_meta.mirror_peer_uuid.empty()) {
        derr << "failed to retrieve mirror peer uuid from remote image pool"
             << dendl;
        r = -ENOENT;
        break;
      }

      librados::IoCtx remote_io_ctx;
      r = librbd::util::create_ioctx(m_remote_io_ctx, "remote image pool",
                                     remote_pool_id, {}, &remote_io_ctx);
      if (r < 0) {
        derr << "failed to open remote image pool " << remote_pool_id << ": "
             << cpp_strerror(r) << dendl;
        if (r == -ENOENT) {
          r = -EINVAL;
        }
        break;
      }

      int64_t local_pool_id = librados::Rados(m_local_io_ctx).pool_lookup(
          remote_io_ctx.get_pool_name().c_str());

      LocalPoolMeta local_pool_meta;
      r = m_pool_meta_cache->get_local_pool_meta(local_pool_id,
                                                 &local_pool_meta);
      if (r < 0 || local_pool_meta.mirror_uuid.empty()) {
        if (r == 0 || r == -ENOENT) {
          r = -EINVAL;
        }
        derr << "failed to retrieve mirror uuid from local image pool" << dendl;
        break;
      }

      r = librbd::util::create_ioctx(m_local_io_ctx, "local image pool",
                                     local_pool_id, {}, &local_io_ctx);
      if (r < 0) {
        derr << "failed to open local image pool " << local_pool_id << ": "
             << cpp_strerror(r) << dendl;
        if (r == -ENOENT) {
          r = -EINVAL;
        }
        break;
      }

      image_replayer = ImageReplayer<I>::create(
        local_io_ctx, m_local_group_ctx, local_pool_meta.mirror_uuid,
        global_image_id, m_threads, m_instance_watcher, m_local_status_updater,
        m_cache_manager_handler, m_pool_meta_cache);

      // TODO only a single peer is currently supported
      image_replayer->add_peer({local_pool_meta.mirror_uuid, remote_io_ctx,
                                remote_pool_meta, m_remote_status_updater});
    }
  }

  if (r < 0) {
    for (auto &[_, image_replayer] : *m_image_replayers) {
      delete image_replayer;
    }
    m_image_replayers->clear();
    return r;
  }

  return 0;
}

template <typename I>
void BootstrapRequest<I>::get_local_group_meta() {
  dout(10) << dendl;

  *m_resync_requested = false;
  librados::ObjectReadOperation op;
  librbd::cls_client::metadata_get_start(&op, RBD_GROUP_RESYNC);

  m_out_bl.clear();

  std::string group_header_oid = librbd::util::group_header_name(
        (*m_state_builder)->local_group_id);
  auto aio_comp = create_rados_callback<
    BootstrapRequest<I>,
    &BootstrapRequest<I>::handle_get_local_group_meta>(this);

  int r = m_local_io_ctx.aio_operate(group_header_oid, aio_comp,
                                     &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_local_group_meta(int r) {
  dout(10) << "r=" << r << dendl;

  std::string data;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::metadata_get_finish(&it, &data);
    if (r == 0) {
      *m_resync_requested = true;
    }
  }
  if (r != -ENOENT){
    // ignore this for now ?
    dout(10) << "failed to get group meta: " << r << dendl;
  }
  if (!*m_resync_requested) {
    finish(0);
    return;
  } else {
    // proceed to remove local group
    remove_local_group();
    return;
  }
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::BootstrapRequest<librbd::ImageCtx>;
