// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupPromoteRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/Utils.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/image/ListWatchersRequest.h"
#include "librbd/mirror/snapshot/GroupUnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"

#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupPromoteRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace {

const uint32_t MAX_RETURN = 1024;

} // anonymous namespace


using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
void GroupPromoteRequest<I>::send() {
  get_mirror_group();
}

template <typename I>
void GroupPromoteRequest<I>::get_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_group_get_start(&op, m_group_id);

  auto comp = create_rados_callback<
      GroupPromoteRequest<I>,
      &GroupPromoteRequest<I>::handle_get_mirror_group>(this);

  m_out_bl.clear();
  int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_get_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_group_get_finish(&iter, &m_mirror_group);
  }

  if (r == -ENOENT) {
    ldout(m_cct, 10) << "group is not enabled for mirroring: " << m_group_name
                   << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror group metadata: "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_last_mirror_snapshot_state();
}

template <typename I>
void GroupPromoteRequest<I>::get_last_mirror_snapshot_state() {
  ldout(m_cct, 10) << dendl;

  auto ctx = new LambdaContext(
    [this](int r) {
      handle_get_last_mirror_snapshot_state(r);
    });
  auto req = librbd::group::ListSnapshotsRequest<>::create(m_group_ioctx, m_group_id,
                                                           true, true,
                                                           &m_snaps, ctx);
  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_get_last_mirror_snapshot_state(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list snapshots for group: " << m_group_name << dendl;
    finish(r);
    return;
  }

  bool mirror_snapshot_exists = false;
  cls::rbd::MirrorSnapshotState state;
  cls::rbd::MirrorGroupSnapshotCompleteState sync;
  cls::rbd::GroupSnapshotState group_snap_state;
  for (auto it = m_snaps.rbegin(); it != m_snaps.rend(); it++) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &it->snapshot_namespace);
    if (ns != nullptr) {
      // XXXMG: check primary_mirror_uuid matches?
      mirror_snapshot_exists = true;
      group_snap_state = it->state;
      state = ns->state;
      sync = ns->complete;
      break;
    }
  }

  if (!mirror_snapshot_exists) {
    group_snap_state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATING;
    sync = cls::rbd::MIRROR_GROUP_SNAPSHOT_INCOMPLETE;
    state = cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED; // XXXMG?
  }

  if (state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
    lderr(m_cct) << "group " << m_group_name << " is already primary" << dendl;
    finish(-EINVAL);
    return;
  } else if (!m_force && (state == cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY ||
                         !is_mirror_group_snapshot_complete(group_snap_state, sync))) {
    lderr(m_cct) << "group " << m_group_name
                 << " is primary within a remote cluster or demotion is not propagated yet"
                 << dendl;
    finish(-EBUSY);
    return;
  }

  get_local_group_meta();
}

template <typename I>
void GroupPromoteRequest<I>::get_local_group_meta() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::metadata_get_start(&op, RBD_GROUP_RESYNC);

  m_out_bl.clear();

  std::string group_header_oid = librbd::util::group_header_name(m_group_id);
  auto aio_comp = create_rados_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_get_local_group_meta>(this);

  int r = m_group_ioctx.aio_operate(group_header_oid, aio_comp,
                                     &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_get_local_group_meta(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::string data;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::metadata_get_finish(&it, &data);
  }
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed reading metadata: " << RBD_GROUP_RESYNC << " : "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (r == 0) {
    ldout(m_cct, 5) << "found previous resync group request, clearing it"
                    << dendl;
    std::string group_header_oid = librbd::util::group_header_name(m_group_id);
    r = cls_client::metadata_remove(&m_group_ioctx, group_header_oid,
                                    RBD_GROUP_RESYNC);
    if (r < 0) {
      lderr(m_cct) << "failed removing metadata: " << RBD_GROUP_RESYNC << " : "
                   << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
  }
  prepare_group_images();
}

template <typename I>
void GroupPromoteRequest<I>::prepare_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_prepare_group_images>(this);

  auto req = snapshot::GroupPrepareImagesRequest<I>::create(m_group_ioctx, m_group_id,
      m_image_ctxs, m_images, &m_mirror_images, nullptr, nullptr, &m_mirror_peer_uuids,
      "promote", m_force, ctx);
  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_prepare_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to prepare group images" << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  if (m_images.empty()) {
    create_primary_group_snapshot();
  } else {
    check_rollback_needed();
  }
}

template <typename I>
void GroupPromoteRequest<I>::check_rollback_needed() {
  if (!m_force) {
    promote_group();
    return;
  }

  ldout(m_cct, 10) << dendl;

  std::vector<cls::rbd::GroupImageSpec> current_images;
  for (const auto& image : m_images) {
    if (image.state == cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED) {
      current_images.push_back(image.spec);
    }
  }

  if (m_snaps.empty()) {
    lderr(m_cct) << "cannot rollback, no mirror group snapshot available on group: "
                 << m_group_name << dendl;
    m_ret_val = -EINVAL;
    close_images();
    return;
  }

  m_need_rollback = false;
  auto snap = m_snaps.rbegin();
  for (; snap != m_snaps.rend(); ++snap) {
    auto mirror_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &snap->snapshot_namespace);
    if (mirror_ns == nullptr || mirror_ns->is_orphan()) {
      continue;
    }

    if (!is_mirror_group_snapshot_complete(snap->state, mirror_ns->complete)) {
      m_need_rollback = true;
      continue;
    }
    break;
  }

  if (snap == m_snaps.rend()) {
    lderr(m_cct) << "cannot rollback, no complete mirror group snapshot available on group: "
                 << m_group_name << dendl;
    m_ret_val = -EINVAL;
    close_images();
    return;
  }

  if (m_need_rollback) {
    // Check for group membership match
    std::vector<cls::rbd::GroupImageSpec> rollback_images;
    for (auto& it : snap->snaps) {
      rollback_images.emplace_back(it.image_id, it.pool);
    }

    if (rollback_images != current_images) {
      lderr(m_cct) << "group membership does not match snapshot membership with rollback_snap_id: "
                   << snap->id << dendl;
      m_ret_val = -EINVAL;
      close_images();
      return;
    }
    m_rollback_group_snap = *snap;
  } else {
    ldout(m_cct, 10) << "no rollback and no orphan snapshot required" << dendl;
  }

  promote_group();
}

template <typename I>
void GroupPromoteRequest<I>::promote_group() {
  ldout(m_cct, 10) <<  dendl;

  bool not_requires_orphan = true;
  m_rollback_snap_ids.resize(m_images.size());
  for (size_t i = 0; i < m_images.size(); i++) {
    bool requires_orphan = false;
    if (!snapshot::util::can_create_primary_snapshot(m_image_ctxs[i], false, true,
                                                     &requires_orphan,
                                                     &m_rollback_snap_ids[i])) {
      lderr(m_cct) << "cannot promote" << dendl;
      m_ret_val = -EINVAL;
      close_images();
      return;
    }
    not_requires_orphan = not_requires_orphan &&
                          (m_rollback_snap_ids[i] == CEPH_NOSNAP && !requires_orphan);
  }

  if (not_requires_orphan && !m_need_rollback) {
    create_primary_group_snapshot();
    return;
  }
  create_group_orphan_snapshot();
}

template <typename I>
void GroupPromoteRequest<I>::create_group_orphan_snapshot() {
  ldout(m_cct, 10) <<  dendl;

  m_orphan_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);

  std::stringstream ind_snap_name_stream;
  auto snap_name = ".mirror.non-primary."+ m_mirror_group.global_group_id +
              "." + m_orphan_group_snap.id;

  m_orphan_group_snap.name = snap_name;

  m_orphan_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATING;

  librados::Rados rados(m_group_ioctx);
  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve min OSD release: " << cpp_strerror(r)
               << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  auto complete = cls::rbd::get_mirror_group_snapshot_complete_initial(require_osd_release);
  m_orphan_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, {}, {}, complete};

  for (auto image_ctx: m_image_ctxs) {
    m_orphan_group_snap.snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                           CEPH_NOSNAP);
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_orphan_group_snap);

  auto aio_comp = create_rados_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_create_group_orphan_snapshot>(this);
  r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_create_group_orphan_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create group orphan snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }
  create_images_orphan_snapshots();
}

template <typename I>
void GroupPromoteRequest<I>::create_images_orphan_snapshots() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_create_images_orphan_snapshots>(this);

  m_orphan_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);
  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto req = snapshot::CreateNonPrimaryRequest<I>::create(
      m_image_ctxs[i], false, "", "", CEPH_NOSNAP, {}, {}, &m_orphan_snap_ids[i], gather_ctx->new_sub());
    req->send();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_create_images_orphan_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create orphan snapshots for images: " << cpp_strerror(r)
                 << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  mark_group_orphan_snapshot_complete();
}

template <typename I>
void GroupPromoteRequest<I>::mark_group_orphan_snapshot_complete() {
  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_orphan_group_snap.snaps[i].snap_id = m_orphan_snap_ids[i];
  }

  std::string group_header_oid = librbd::util::group_header_name(m_group_id);

  m_orphan_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(m_orphan_group_snap);

  int r = cls_client::group_snap_set(&m_group_ioctx, group_header_oid, m_orphan_group_snap);
  if (r < 0) {
    lderr(m_cct) << "failed to mark orphan snapshot complete: " << cpp_strerror(r)
               << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  const size_t num_images = m_images.size();
  // Resize all per-image state vectors required for check_watchers
  m_watchers.resize(num_images);
  m_update_watcher_handles.resize(num_images, 0);
  m_scheduler_ticks.resize(num_images, 0);
  m_locks_acquired.resize(num_images, false);

  // Initialize the vector of UpdateWatchCtx pointers
  for (size_t i = 0; i < num_images; ++i) {
    m_update_watch_ctxs.push_back(new UpdateWatchCtx(this, i));
  }

  check_watchers();
}

template <typename I>
void GroupPromoteRequest<I>::check_watchers() {
  ldout(m_cct, 10) << "image_index=" << m_image_index << dendl;

  if (m_image_index >= m_image_ctxs.size()) {
    // unregistered watchers for all images
    // move to the acquire_exclusive_locks
    acquire_exclusive_locks();
    return;
  }

  // Start the list watchers flow for the current image index
  list_watchers(m_image_index);
}

template <typename I>
void GroupPromoteRequest<I>::list_watchers(size_t index) {
  ldout(m_cct, 10) << "image_index=" << index << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_list_watchers>(this);

  m_watchers[index].clear();
  auto flags = librbd::image::LIST_WATCHERS_FILTER_OUT_MY_INSTANCE |
               librbd::image::LIST_WATCHERS_MIRROR_INSTANCES_ONLY;

  auto req = librbd::image::ListWatchersRequest<I>::create(
    *m_image_ctxs[index], flags, &m_watchers[index], ctx);
  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_list_watchers(int r) {
  size_t index = m_image_index;
  ldout(m_cct, 10) << "image_index=" << index << ", r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "image " << index << ": failed to list watchers: "
               << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  if (m_watchers[index].empty()) {
    // watchers cleared for current image
    // move to the next image
    m_image_index++;
    check_watchers();
    return;
  }

  wait_update_notify(index);
}

template <typename I>
void GroupPromoteRequest<I>::wait_update_notify(size_t index) {
  CephContext *cct = m_cct;
  I *image_ctx = m_image_ctxs[index];
  ldout(cct, 15) << "image_index=" << index << dendl;

  ImageCtx::get_timer_instance(cct, &m_timer, &m_timer_lock);

  std::lock_guard timer_lock{*m_timer_lock};

  m_scheduler_ticks[index] = 5;

  int r = image_ctx->state->register_update_watcher(m_update_watch_ctxs[index],
                                                    &m_update_watcher_handles[index]);
  if (r < 0) {
    lderr(cct) << "image " << index << ": failed to register update watcher: "
               << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  scheduler_unregister_update_watcher(index);
}

template <typename I>
void GroupPromoteRequest<I>::handle_update_notify(size_t index) {
  CephContext *cct = m_cct;
  ldout(cct, 15) << "image_index=" << index << dendl;

  std::lock_guard timer_lock{*m_timer_lock};
  m_scheduler_ticks[index] = 0;
}

template <typename I>
void GroupPromoteRequest<I>::scheduler_unregister_update_watcher(size_t index) {
  ceph_assert(ceph_mutex_is_locked(*m_timer_lock));

  CephContext *cct = m_cct;
  I *image_ctx = m_image_ctxs[index];
  ldout(cct, 15) << "image_index=" << index << ", scheduler_ticks="
                 << m_scheduler_ticks[index] << dendl;

  if (m_scheduler_ticks[index] > 0) {
    m_scheduler_ticks[index]--;
    m_timer->add_event_after(1, new LambdaContext([this, index](int) {
        scheduler_unregister_update_watcher(index);
      }));
    return;
  }

  image_ctx->op_work_queue->queue(new LambdaContext([this, index](int) {
      unregister_update_watcher(index);
    }), 0);
}

template <typename I>
void GroupPromoteRequest<I>::unregister_update_watcher(size_t index) {
  CephContext *cct = m_cct;
  I *image_ctx = m_image_ctxs[index];
  ldout(cct, 15) << "image_index=" << index << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_unregister_update_watcher>(this);

  image_ctx->state->unregister_update_watcher(m_update_watcher_handles[index], ctx);
}

template <typename I>
void GroupPromoteRequest<I>::handle_unregister_update_watcher(int r) {
  size_t index = m_image_index;
  CephContext *cct = m_cct;
  ldout(cct, 15) << "image_index=" << index << ", r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "image " << index << ": failed to unregister update watcher: "
               << cpp_strerror(r) << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  list_watchers(index);
}

template <typename I>
void GroupPromoteRequest<I>::acquire_exclusive_locks() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_acquire_exclusive_locks>(this);
  m_excl_locks_acquired = true;
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    {
      std::unique_lock locker{m_image_ctxs[i]->owner_lock};
      if (m_image_ctxs[i]->exclusive_lock != nullptr &&
          !m_image_ctxs[i]->exclusive_lock->is_lock_owner()) {
        CephContext *cct = m_image_ctxs[i]->cct;
        ldout(cct, 15) << dendl;
        m_locks_acquired[i] = true;
        m_image_ctxs[i]->exclusive_lock->block_requests(0);
        m_image_ctxs[i]->exclusive_lock->acquire_lock(gather_ctx->new_sub());
      }
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_acquire_exclusive_locks(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to acquire exclusive lock: " << cpp_strerror(r)
               << dendl;
    m_ret_val = r;
    release_exclusive_locks();
    return;
  } else {
    for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
      std::unique_lock locker{m_image_ctxs[i]->owner_lock};
      if (m_image_ctxs[i]->exclusive_lock != nullptr &&
          !m_image_ctxs[i]->exclusive_lock->is_lock_owner()) {
        lderr(m_cct) << "failed to acquire exclusive lock" << dendl;
        r = m_image_ctxs[i]->exclusive_lock->get_unlocked_op_error();
        m_ret_val = r;
        locker.unlock();
        release_exclusive_locks();
        return;
      }
    }
  }

  rollback();
}

template <typename I>
void GroupPromoteRequest<I>::rollback() {
  if(!m_need_rollback) {
    create_primary_group_snapshot();
    return;
  }

  ldout(m_cct, 10) << dendl;
  auto ctx = create_context_callback<
        GroupPromoteRequest<I>, &GroupPromoteRequest<I>::handle_rollback>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    std::shared_lock owner_locker{m_image_ctxs[i]->owner_lock};
    std::shared_lock image_locker{m_image_ctxs[i]->image_lock};

    auto info = m_image_ctxs[i]->get_snap_info(m_rollback_group_snap.snaps[i].snap_id);
    ceph_assert(info != nullptr);
    auto snap_namespace = info->snap_namespace;
    auto snap_name = info->name;

    image_locker.unlock();

    m_image_ctxs[i]->operations->execute_snap_rollback(snap_namespace, snap_name,
                                                       m_progress_ctx, gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_rollback(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to rollback: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    release_exclusive_locks();
    return;
  }

  create_primary_group_snapshot();
}

template <typename I>
void GroupPromoteRequest<I>::create_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.id = librbd::util::generate_image_id(m_group_ioctx);

  auto snap_name = ".mirror.primary." + m_mirror_group.global_group_id
                + "." + m_group_snap.id;
  m_group_snap.name = snap_name;

  cls::rbd::MirrorSnapshotState state = cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY;
  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATING;

  librados::Rados rados(m_group_ioctx);
  int8_t require_osd_release;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve min OSD release: " << cpp_strerror(r)
               << dendl;
    m_ret_val = r;
    close_images();
    return;
  }

  auto complete = cls::rbd::get_mirror_group_snapshot_complete_initial(require_osd_release);
  m_group_snap.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    state, m_mirror_peer_uuids, {}, {}, complete};

  for (auto image_ctx: m_image_ctxs) {
    m_group_snap.snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                    CEPH_NOSNAP);
  }

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_create_primary_group_snapshot>(this);
  r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_create_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    remove_primary_group_snapshot();
    return;
  }

  create_images_primary_snapshots();
}

template <typename I>
void GroupPromoteRequest<I>::create_images_primary_snapshots() {
  if (m_images.empty()) {
    update_primary_group_snapshot();
    return;
  }

  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_create_images_primary_snapshots>(this);

  m_snap_ids.resize(m_image_ctxs.size(), CEPH_NOSNAP);
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_global_image_ids.push_back(m_mirror_images[i].global_image_id);
  }

  auto req = snapshot::GroupImageCreatePrimaryRequest<I>::create(
    m_cct, m_image_ctxs, m_global_image_ids, SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE,
    (snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS |
      snapshot::CREATE_PRIMARY_FLAG_FORCE), m_group_snap.id, &m_snap_ids, !m_excl_locks_acquired, ctx);
  req->send();
}


template <typename I>
void GroupPromoteRequest<I>::handle_create_images_primary_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_group_snap.snaps[i].snap_id = m_snap_ids[i];
  }

  if (r < 0) {
    lderr(m_cct) << "failed to create primary mirror image snapshots: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    remove_primary_group_snapshot();
    return;
  }

  disable_non_primary_features();
}

template <typename I>
void GroupPromoteRequest<I>::disable_non_primary_features() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_disable_non_primary_features>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  // remove the non-primary feature flag so that the image can be
  // R/W by standard RBD clients
  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    I *image_ctx = m_image_ctxs[i];

    librados::ObjectWriteOperation op;
    cls_client::set_features(&op, 0U, RBD_FEATURE_NON_PRIMARY);

    auto on_disable = new LambdaContext(
      [this, new_sub_ctx = gather_ctx->new_sub()](int r) {
        if (r < 0) {
          lderr(m_cct) << "failed to disable non-primary feature: "
                       << cpp_strerror(r) << dendl;
        }
        new_sub_ctx->complete(r);
      });

    auto aio_comp = create_rados_callback(on_disable);

    int r = image_ctx->md_ctx.aio_operate(image_ctx->header_oid, aio_comp, &op);
    ceph_assert(r == 0);
    aio_comp->release();
  }
  gather_ctx->activate();
}


template <typename I>
void GroupPromoteRequest<I>::handle_disable_non_primary_features(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable non-primary feature: "
               << cpp_strerror(r) << dendl;
    m_ret_val = r;
    remove_primary_group_snapshot();
    return;
  }

  update_primary_group_snapshot();
}

template <typename I>
void GroupPromoteRequest<I>::update_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  m_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(m_group_snap);

  librados::ObjectWriteOperation op;
  cls_client::group_snap_set(&op, m_group_snap);

  auto aio_comp = create_rados_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_update_primary_group_snapshot>(this);
  int r = m_group_ioctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                    aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupPromoteRequest<I>::handle_update_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update primary group snapshot: "
                 << cpp_strerror(r) << dendl;
    m_ret_val = r;
    group_unlink_peer();
    return;
  }

  release_exclusive_locks();
}

template <typename I>
void GroupPromoteRequest<I>::release_exclusive_locks() {
  ldout(m_cct, 10) << dendl;
  if(!m_excl_locks_acquired) {
    close_images();
    return;
  }
  auto ctx = librbd::util::create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_release_exclusive_locks>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (size_t i = 0; i < m_image_ctxs.size(); ++i) {
    if (m_locks_acquired[i]) {
      std::unique_lock locker{m_image_ctxs[i]->owner_lock};
      if (m_image_ctxs[i]->exclusive_lock != nullptr) {
        m_image_ctxs[i]->exclusive_lock->unblock_requests();
        m_image_ctxs[i]->exclusive_lock->release_lock(gather_ctx->new_sub());
      }
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_release_exclusive_locks(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to release exclusive locks for images: "
                 << cpp_strerror(r) << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }
  close_images();
}

template <typename I>
void GroupPromoteRequest<I>::group_unlink_peer() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_group_unlink_peer>(this);

  auto req = mirror::snapshot::GroupUnlinkPeerRequest<I>::create(
        m_group_ioctx, m_group_id, &m_mirror_peer_uuids, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_group_unlink_peer(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to unlink mirror group snapshot: " << cpp_strerror(r)
                 << dendl;
  }
  release_exclusive_locks();
}

template <typename I>
void GroupPromoteRequest<I>::remove_primary_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>,
    &GroupPromoteRequest<I>::handle_remove_primary_group_snapshot>(this);

  auto req = snapshot::RemoveGroupSnapshotRequest<I>::create(m_group_ioctx,
     m_group_id, &m_group_snap, &m_image_ctxs, ctx);

  req->send();
}

template <typename I>
void GroupPromoteRequest<I>::handle_remove_primary_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove mirror group snapshot: " << cpp_strerror(r)
                 << dendl;
  }
  release_exclusive_locks();
}

template <typename I>
void GroupPromoteRequest<I>::close_images() {
  if (m_image_ctxs.empty()) {
    finish(m_ret_val);
    return;
  }

  ldout(m_cct, 10) << dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
      m_image_ctxs[i]->image_lock.lock();
      m_image_ctxs[i]->read_only_mask |= IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
      m_image_ctxs[i]->image_lock.unlock();

      m_image_ctxs[i]->state->handle_update_notification();
  }

  auto ctx = create_context_callback<
    GroupPromoteRequest<I>, &GroupPromoteRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    if (ictx != nullptr) {
      ictx->state->close(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPromoteRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to close images: " << cpp_strerror(r) << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }

  finish(m_ret_val);
}

template <typename I>
void GroupPromoteRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupPromoteRequest<librbd::ImageCtx>;
