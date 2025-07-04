// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "Replayer.h"
#include "GroupMirrorStateUpdateRequest.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "include/stringify.h"
#include "common/Timer.h"
#include "cls/rbd/cls_rbd_client.h"
#include "json_spirit/json_spirit.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/Threads.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::Replayer: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

namespace {

const uint32_t MAX_RETURN = 1024;

const cls::rbd::GroupSnapshot* get_latest_group_snapshot(
    const std::vector<cls::rbd::GroupSnapshot>& gp_snaps) {
  auto it = gp_snaps.rbegin();
  if (it != gp_snaps.rend()) {
    return &*it;
  }
  return nullptr;
}

const cls::rbd::GroupSnapshot* get_latest_mirror_group_snapshot(
    const std::vector<cls::rbd::GroupSnapshot>& gp_snaps) {
  for (auto it = gp_snaps.rbegin(); it != gp_snaps.rend(); ++it) {
    if (cls::rbd::get_group_snap_namespace_type(it->snapshot_namespace) ==
         cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
      return &*it;
    }
  }
  return nullptr;
}

const cls::rbd::GroupSnapshot* get_latest_complete_mirror_group_snapshot(
    const std::vector<cls::rbd::GroupSnapshot>& gp_snaps) {
  for (auto it = gp_snaps.rbegin(); it != gp_snaps.rend(); ++it) {
    if (it->state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE &&
        (cls::rbd::get_group_snap_namespace_type(it->snapshot_namespace) ==
         cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR)) {
      return &*it;
    }
  }
  return nullptr;
}

bool is_group_primary(const std::vector<cls::rbd::GroupSnapshot>& gp_snaps) {
  if (gp_snaps.empty()) {
    return false;
  }

  auto gp_snap = gp_snaps.rbegin();
  auto gp_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &gp_snap->snapshot_namespace);
  if (gp_snap_ns &&
      gp_snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
    return true;
  }

  return false;
}

int get_group_snapshot_timestamp(librados::IoCtx& group_ioctx,
                                 const cls::rbd::GroupSnapshot& group_snap,
                                 utime_t* timestamp) {
  // Set timestamp for an empty group snapshot as zero
  if (group_snap.snaps.empty()) {
    *timestamp = utime_t(0, 0);
    return 0;
  }

  cls::rbd::SnapshotInfo image_snap_info;
  std::vector<utime_t> timestamps;
  for (const auto& image_snap : group_snap.snaps) {
    // TODO: Fetch the member image's snapshot info using the member image's
    // IoCtx. Below assumes that the member images are in the group's pool.
    int r = librbd::cls_client::snapshot_get(
      &group_ioctx, librbd::util::header_name(image_snap.image_id),
      image_snap.snap_id, &image_snap_info);
    if (r < 0) {
      return r;
    }
    timestamps.push_back(image_snap_info.timestamp);
  }
  *timestamp = *std::min_element(timestamps.begin(), timestamps.end());
  return 0;
}

std::string prepare_non_primary_mirror_snap_name(
    const std::string &global_group_id,
    const std::string &snap_id) {
  std::stringstream ind_snap_name_stream;
  ind_snap_name_stream << ".mirror.non-primary."
                       << global_group_id << "." << snap_id;
  return ind_snap_name_stream.str();
}

} // anonymous namespace

template <typename I>
Replayer<I>::Replayer(
    Threads<I>* threads,
    librados::IoCtx &local_io_ctx,
    librados::IoCtx &remote_io_ctx,
    const std::string &global_group_id,
    const std::string& local_mirror_uuid,
    PoolMetaCache* pool_meta_cache,
    std::string local_group_id,
    std::string remote_group_id,
    GroupCtx *local_group_ctx,
    std::list<std::pair<librados::IoCtx, ImageReplayer<I> *>> *image_replayers)
  : m_threads(threads),
    m_local_io_ctx(local_io_ctx),
    m_remote_io_ctx(remote_io_ctx),
    m_global_group_id(global_group_id),
    m_local_mirror_uuid(local_mirror_uuid),
    m_pool_meta_cache(pool_meta_cache),
    m_local_group_id(local_group_id),
    m_remote_group_id(remote_group_id),
    m_local_group_ctx(local_group_ctx),
    m_image_replayers(image_replayers),
    m_lock(ceph::make_mutex(librbd::util::unique_lock_name(
      "rbd::mirror::group_replayer::Replayer", this))) {
  dout(10) << m_global_group_id <<  dendl;
}

template <typename I>
Replayer<I>::~Replayer() {
  dout(10) << m_global_group_id << dendl;

  ceph_assert(m_state == STATE_COMPLETE);
}

template <typename I>
bool Replayer<I>::is_replay_interrupted(std::unique_lock<ceph::mutex>* locker) {

  if (m_state == STATE_COMPLETE || m_stop_requested) {
    locker->unlock();
    return true;
  }

  return false;
}

template <typename I>
void Replayer<I>::schedule_load_group_snapshots() {

  std::lock_guard timer_locker{m_threads->timer_lock};
  std::lock_guard locker{m_lock};

  if (m_state != STATE_REPLAYING) {
    return;
  }

  dout(10) << dendl;

  ceph_assert(m_load_snapshots_task == nullptr);
  m_load_snapshots_task = create_context_callback<
    Replayer<I>,
    &Replayer<I>::handle_schedule_load_group_snapshots>(this);

  m_threads->timer->add_event_after(5, m_load_snapshots_task);
}

template <typename I>
void Replayer<I>::handle_schedule_load_group_snapshots(int r) {
  dout(10) << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_threads->timer_lock));

  {
    std::unique_lock locker{m_lock};
    if (m_state != STATE_REPLAYING) {
      return;
    }
  }

  ceph_assert(m_load_snapshots_task != nullptr);
  m_load_snapshots_task = nullptr;

  auto ctx = new LambdaContext(
    [this](int r) {
      validate_local_group_snapshots();
      m_in_flight_op_tracker.finish_op();
    });
  m_in_flight_op_tracker.start_op();
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void Replayer<I>::cancel_load_group_snapshots() {
  dout(10) << dendl;

  std::unique_lock timer_locker{m_threads->timer_lock};
  if (m_load_snapshots_task != nullptr) {
    dout(10) << dendl;

    if (m_threads->timer->cancel_event(m_load_snapshots_task)) {
      m_load_snapshots_task = nullptr;
    }
  }
}

template <typename I>
void Replayer<I>::handle_replay_complete(std::unique_lock<ceph::mutex>* locker,
                                         int r, const std::string &desc) {
  dout(10) << "r=" << r << ", desc=" << desc << dendl;

  if (m_error_code == 0) {
    m_error_code = r;
    m_error_description = desc;
  }

  if (m_state != STATE_REPLAYING && m_state != STATE_IDLE) {
    return;
  }

  m_stop_requested = true;
  m_state = STATE_COMPLETE;
  notify_group_listener();
}

template <typename I>
void Replayer<I>::notify_group_listener() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  dout(10) << dendl;

  Context *ctx = new LambdaContext([this](int) {
      m_local_group_ctx->listener->handle_notification();
      m_in_flight_op_tracker.finish_op();
      });
  m_in_flight_op_tracker.start_op();
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void Replayer<I>::local_group_image_list_by_id(bufferlist* out_bl_ptr,
    std::vector<cls::rbd::GroupImageStatus>* local_images_ptr,
    Context* on_finish) {

  // input validation
  ceph_assert(out_bl_ptr != nullptr);
  ceph_assert(local_images_ptr != nullptr);
  ceph_assert(on_finish != nullptr);

  librados::ObjectReadOperation op;
  cls::rbd::GroupImageSpec start_after;

  if (!local_images_ptr->empty()) {
    start_after = local_images_ptr->rbegin()->spec;
  }

  librbd::cls_client::group_image_list_start(&op, start_after, MAX_RETURN);
  out_bl_ptr->clear();

  auto aio_comp = create_rados_callback(
    new LambdaContext([this, out_bl_ptr, local_images_ptr, on_finish](int r) {
      handle_local_group_image_list_by_id(r, out_bl_ptr, local_images_ptr,
                                          on_finish);
    }));

  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), aio_comp, &op,
                                      out_bl_ptr);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void Replayer<I>::handle_local_group_image_list_by_id(
    int r, bufferlist* out_bl_ptr,
    std::vector<cls::rbd::GroupImageStatus>* local_images_ptr,
    Context* on_finish) {
  dout(10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> image_id_pages;
  if (r == 0) {
    auto iter = out_bl_ptr->cbegin();
    r = librbd::cls_client::group_image_list_finish(&iter, &image_id_pages);
  }

  if (r < 0) {
    derr << "error listing local group images: " << cpp_strerror(r)
         << dendl;
    on_finish->complete(r);
    return;
  }

  // reserve space and append new images
  local_images_ptr->reserve(local_images_ptr->size() + image_id_pages.size());
  local_images_ptr->insert(local_images_ptr->end(),
      std::make_move_iterator(image_id_pages.begin()),
      std::make_move_iterator(image_id_pages.end()));

  if (image_id_pages.size() == MAX_RETURN) {
    local_group_image_list_by_id(out_bl_ptr, local_images_ptr, on_finish);
    return;
  }

  dout(10) << "completed listing local images: count="
           << local_images_ptr->size() << dendl;
  on_finish->complete(0);
}

template <typename I>
void Replayer<I>::validate_image_snaps_sync_complete(
    const cls::rbd::GroupSnapshot &local_snap, Context *on_finish) {
  dout(10) << "group snap_id: " << local_snap.id << dendl;

  if (m_snapshot_start.is_zero()) {
    // The mirror group snapshot's start time being zero and the group snapshot
    // being incomplete indicate that the mirror daemon was restarted. So reset
    // the mirror group snap's start time that is used to calculate the total
    // time taken to complete the syncing of the mirror group snap as best as
    // we can.
    m_snapshot_start = ceph_clock_now();
  }

  auto snap_type = cls::rbd::get_group_snap_namespace_type(
      local_snap.snapshot_namespace);

  if (snap_type != cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
    regular_snapshot_complete(local_snap.id, on_finish);
  } else {
    mirror_snapshot_complete(local_snap.id, on_finish);
  }
}

template <typename I>
void Replayer<I>::init(Context* on_finish) {
  dout(10) << "m_global_group_id=" << m_global_group_id << dendl;

  ceph_assert(m_state == STATE_INIT);

  RemotePoolMeta remote_pool_meta;
  int r = m_pool_meta_cache->get_remote_pool_meta(
    m_remote_io_ctx.get_id(), &remote_pool_meta);
  if (r < 0 || remote_pool_meta.mirror_peer_uuid.empty()) {
    derr << "failed to retrieve mirror peer uuid from remote pool" << dendl;
    m_state = STATE_COMPLETE;
    m_threads->work_queue->queue(on_finish, r);
    return;
  }

  m_remote_mirror_uuid = remote_pool_meta.mirror_uuid;
  m_remote_mirror_peer_uuid = remote_pool_meta.mirror_peer_uuid;
  dout(10) << "remote_mirror_uuid=" << m_remote_mirror_uuid
           << ", remote_mirror_peer_uuid=" << m_remote_mirror_peer_uuid << dendl;

  on_finish->complete(0);

  m_update_group_state = true;
  validate_local_group_snapshots();
}

template <typename I>
void Replayer<I>::validate_local_group_snapshots() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;

  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker) || m_stop_requested) {
    return;
  }

  if (m_state != STATE_COMPLETE) {
    m_state = STATE_REPLAYING;
  }

  // early exit if no snapshots to process
  if (m_local_group_snaps.empty()) {
    load_local_group_snapshots(&locker);
    return;
  }

  m_in_flight_op_tracker.start_op();
  locker.unlock();

  // prepare gather context for async operations
  auto ctx = new LambdaContext([this](int) {
    std::unique_lock locker{m_lock};
    m_in_flight_op_tracker.finish_op();
    load_local_group_snapshots(&locker);
  });

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);

  // process snapshots requiring validation
  for (auto &local_snap : m_local_group_snaps) {
    // skip validation for already complete snapshots
    if (local_snap.state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
      continue;
    }

    // skip validation for primary snapshots
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &local_snap.snapshot_namespace);
    if (ns != nullptr && ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
      continue;
    }

    // setup validation callback
    Context *sub_ctx = gather_ctx->new_sub();
    auto ctx = new LambdaContext([this, sub_ctx](int r) {
      if (r < 0) {
        std::unique_lock locker{m_lock};
        m_retry_validate_snap = true;
      }
      sub_ctx->complete(0);
    });

    // validate incomplete non-primary mirror or regular snapshots
    validate_image_snaps_sync_complete(local_snap, ctx);
  }
  gather_ctx->activate();

}

template <typename I>
void Replayer<I>::load_local_group_snapshots(
    std::unique_lock<ceph::mutex>* locker) {
  if (is_replay_interrupted(locker)) {
    return;
  }

  m_in_flight_op_tracker.start_op();
  m_local_group_snaps.clear();

  auto ctx = new LambdaContext(
    [this] (int r) {
      handle_load_local_group_snapshots(r);
      m_in_flight_op_tracker.finish_op();
  });

  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_local_io_ctx,
      m_local_group_id, true, true, &m_local_group_snaps, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_load_local_group_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker)) {
    return;
  }

  if (r < 0) {
    derr << "error listing local mirror group snapshots: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(&locker, r, "failed to list local group snapshots");
    return;
  }

  auto last_local_snap = get_latest_mirror_group_snapshot(m_local_group_snaps);
  if (last_local_snap != nullptr) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &last_local_snap->snapshot_namespace);
    if (ns->state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
      if (ns->is_orphan()) {
        dout(5) << "local group being force promoted" << dendl;
        handle_replay_complete(&locker, 0, "orphan (force promoting)");
        return;
      } else if (m_retry_validate_snap ||
                 last_local_snap->state == cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE) {
        m_retry_validate_snap = false;
        locker.unlock();
        schedule_load_group_snapshots();
        return; // previous snap is syncing, reload local snaps to confirm its completeness.
      }
    } else { // primary
      if (last_local_snap->state == cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE) {
        dout(10) << "found incomplete primary group snapshot: "
                 << last_local_snap->id << dendl;
        locker.unlock();
        schedule_load_group_snapshots();
        return;
      }
      handle_replay_complete(&locker, 0, "local group is primary");
      return;
    }
  }

  load_remote_group_snapshots();
}

template <typename I>
void Replayer<I>::load_remote_group_snapshots() {
  dout(10) << "m_remote_group_id=" << m_remote_group_id << dendl;

  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  m_in_flight_op_tracker.start_op();
  m_remote_group_snaps.clear();

  auto ctx = new LambdaContext(
    [this] (int r) {
      handle_load_remote_group_snapshots(r);
      m_in_flight_op_tracker.finish_op();
  });

  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_remote_io_ctx,
      m_remote_group_id, true, true, &m_remote_group_snaps, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_load_remote_group_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker)) {
    return;
  }

  if (r < 0) {  // may be remote group is deleted?
    derr << "error listing remote mirror group snapshots: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(&locker, r, "failed to list remote group snapshots");
    return;
  }

  for (auto remote_snap = m_remote_group_snaps.begin();
       remote_snap != m_remote_group_snaps.end(); ) {
    auto remote_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &remote_snap->snapshot_namespace);
    if (remote_snap_ns == nullptr) {
      ++remote_snap;
      continue;
    }
    if (remote_snap_ns->mirror_peer_uuids.count(m_remote_mirror_peer_uuid) == 0) {
      dout(10) << "filtering out snapshot " << remote_snap->id
               << " with no matching mirror_peer_uuid in: "
               << remote_snap_ns->mirror_peer_uuids << " (expected: "
               << m_remote_mirror_peer_uuid << ")" << dendl;
      remote_snap = m_remote_group_snaps.erase(remote_snap);
    } else {
      ++remote_snap;
    }
  }
  is_resync_requested();
}

template <typename I>
void Replayer<I>::is_resync_requested() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::metadata_get_start(&op, RBD_GROUP_RESYNC);

  m_out_bl.clear();

  std::string group_header_oid = librbd::util::group_header_name(
      m_local_group_id);
  m_in_flight_op_tracker.start_op();
  auto aio_comp = create_rados_callback(
    new LambdaContext([this](int r) {
      handle_is_resync_requested(r);
      m_in_flight_op_tracker.finish_op();
    }));

  int r = m_local_io_ctx.aio_operate(group_header_oid, aio_comp,
                                     &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void Replayer<I>::handle_is_resync_requested(int r) {
  std::unique_lock locker{m_lock};
  dout(10) << "r=" << r << dendl;

  if (is_replay_interrupted(&locker)) {
    return;
  }
  std::string value;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::metadata_get_finish(&it, &value);
  }
  if (r < 0 && r != -ENOENT) {
    derr << "failed reading metadata: " << cpp_strerror(r) << dendl;
  } else if (r == 0) {
    dout(10) << "local group resync requested" << dendl;
    if (is_group_primary(m_remote_group_snaps)) {
      handle_replay_complete(&locker, 0, "resync requested");
      return;
    }
    dout(10) << "cannot resync as remote is not primary" << dendl;
  }

  is_rename_requested();
}

template <typename I>
void Replayer<I>::is_rename_requested() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, m_remote_group_id);
  m_out_bl.clear();
  m_in_flight_op_tracker.start_op();
  auto comp = create_rados_callback(
    new LambdaContext([this](int r) {
      handle_is_rename_requested(r);
      m_in_flight_op_tracker.finish_op();
    }));

  int r = m_remote_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op,
                                      &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void Replayer<I>::handle_is_rename_requested(int r) {
  std::unique_lock locker{m_lock};
  dout(10) << "r=" << r << dendl;

  if (is_replay_interrupted(&locker)) {
    return;
  }
  std::string remote_group_name;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_name_finish(&iter, &remote_group_name);
  }
  if (r < 0) {
    derr << "failed to retrieve remote group name: " << cpp_strerror(r)
         << dendl;
  } else if (r == 0 && m_local_group_ctx &&
             m_local_group_ctx->name != remote_group_name) {
    dout(10) << "remote group renamed" << dendl;
    handle_replay_complete(&locker, 0, "remote group renamed");
    return;
  }
  check_local_group_snapshots(&locker);
}

template <typename I>
void Replayer<I>::check_local_group_snapshots(
    std::unique_lock<ceph::mutex>* locker) {
  if (!m_local_group_snaps.empty()) {
    prune_group_snapshots(locker);
    auto last_local_snap = get_latest_group_snapshot(m_local_group_snaps);
    auto last_local_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &last_local_snap->snapshot_namespace);
    if (last_local_snap_ns &&
        last_local_snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED &&
        !m_remote_group_snaps.empty()) {
      if (last_local_snap->id == m_remote_group_snaps.rbegin()->id) {
        handle_replay_complete(locker, -EREMOTEIO, "remote group demoted");
        return;
      }
    } else if (last_local_snap_ns &&
     last_local_snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED) {
      bool split_brain = true;
      for (auto it = m_remote_group_snaps.begin();
           it != m_remote_group_snaps.end(); it++) {
        auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
             &it->snapshot_namespace);
        if (ns == nullptr ||
            it->id != last_local_snap->id) {
          continue;
        }
        if (ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED) {
          split_brain = false;
          break;
        }
      }
      if (split_brain) {
        handle_replay_complete(locker, -EEXIST, "split-brain");
        return;
      }
    }
  }

  scan_for_unsynced_group_snapshots(locker);
}

template <typename I>
void Replayer<I>::scan_for_unsynced_group_snapshots(
    std::unique_lock<ceph::mutex>* locker) {
  dout(10) << dendl;

  // Need to check for this because we do unlock and lock while prune so
  // there is a small window.
  if (is_replay_interrupted(locker)) {
    return;
  }

  auto last_local_snap = get_latest_group_snapshot(m_local_group_snaps);
  // check if we have a matching snap on remote to start with it.
  if (last_local_snap != nullptr) {
    try_create_group_snapshot(last_local_snap->id, locker);
  } else { // empty local cluster, started mirroring freshly
    try_create_group_snapshot("", locker);
  }

  if (m_stop_requested) {
    // stop group replayer
    handle_replay_complete(locker, 0, "");
    return;
  }
  locker->unlock();
  schedule_load_group_snapshots();
}

template <typename I>
void Replayer<I>::try_create_group_snapshot(
    std::string prev_snap_id, std::unique_lock<ceph::mutex>* locker) {
  if (m_remote_group_snaps.empty()) {
    dout(10) << "remote snapshots are empty" << dendl;
    return;
  }

  bool found = false;
  auto snap = m_remote_group_snaps.end();
  for (auto remote_snap = m_remote_group_snaps.begin();
      remote_snap != m_remote_group_snaps.end(); ++remote_snap) {
    if (prev_snap_id.empty() ||
        (found || prev_snap_id == remote_snap->id)) {
      found = true;
      snap = remote_snap; // sync this snapshot
      if (!prev_snap_id.empty()) {
        snap = std::next(remote_snap); // attempt to sync next remote snapshot
      }
      if (snap != m_remote_group_snaps.end() &&
          snap->state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
        auto next_remote_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
            &snap->snapshot_namespace);
        if (next_remote_snap_ns == nullptr) {
          dout(10) << "found remote user group snapshot: "
                   << snap->id << dendl;
          create_group_snapshot(*snap, locker);
          continue;
        } else if (next_remote_snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY ||
            next_remote_snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED) {
          dout(10) << "found primary remote mirror group snapshot: "
                   << snap->id << dendl;
          create_group_snapshot(*snap, locker);
          return;
        } else {
          dout(10) << "skipping non-primary remote group snapshot: "
                   << snap->id << dendl;
          continue;
        }
      } else {
        dout(10) << "all remote snaps synced" << dendl;
        return;
      }
    }
  }

  if (!prev_snap_id.empty() && !found) {
    dout(10) << "none of the local snaps match remote" << dendl;
    handle_replay_complete(locker, -EEXIST, "split-brain");
  }
}

template <typename I>
void Replayer<I>::create_group_snapshot(cls::rbd::GroupSnapshot snap,
                                        std::unique_lock<ceph::mutex>* locker) {
  dout(10) << snap.id << dendl;

  if (m_snapshot_start.is_zero()) {
    m_snapshot_start = ceph_clock_now();
  }

  auto snap_type = cls::rbd::get_group_snap_namespace_type(
      snap.snapshot_namespace);
  if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
    const auto& snap_ns = std::get<cls::rbd::GroupSnapshotNamespaceMirror>(
        snap.snapshot_namespace);

    if (snap_ns.is_non_primary()) {
      dout(10) << "remote group snapshot " << snap.id << " is non primary"
               << dendl;
      return;
    }

    auto snap_state =
      snap_ns.state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY ?
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY :
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED;

    m_in_flight_op_tracker.start_op();

    auto ctx = new LambdaContext([this, snap](int r) mutable {
      std::unique_lock locker{m_lock};
      if (r < 0) {
        dout(10) << "create mirror snapshot failed, will be retried later: "
                 << cpp_strerror(r) << dendl;
        m_in_flight_op_tracker.finish_op();
        return;
      }
      if (m_update_group_state) {
        update_local_group_state(std::move(snap));
      } else {
        // if m_replayer in the ImageReplayer is null this cannot be forwarded.
        // May be we should retry this setting in the validate_image_snaps_sync_complete().
        // Same for image_replayer->prune_snapshot(); setting actually!!!!
        set_image_replayer_limits("", &snap, &locker);
      }
      m_in_flight_op_tracker.finish_op();
    });

    create_mirror_snapshot(&snap, snap_state, ctx);
  } else if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
    auto itr = std::find_if(
        m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
        [&snap](const cls::rbd::GroupSnapshot &s) {
        return s.id == snap.id;
        });

    if (itr == m_remote_group_snaps.end()) {
      dout(10) << "remote group snapshot not found: " << snap.id << dendl;
      return;
    }

    auto next_remote_snap = std::next(itr);
    if (next_remote_snap == m_remote_group_snaps.end()) {
      return;
    }

    // check if we have a valid mirror snapshot to proceed
    bool can_proceed = false;
    for (; next_remote_snap != m_remote_group_snaps.end(); ++next_remote_snap) {
      auto next_snap_type = cls::rbd::get_group_snap_namespace_type(
          next_remote_snap->snapshot_namespace);

      if (next_snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
        continue; // skip user snapshots
      } else if (next_remote_snap->state == cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE) {
        dout(10) << "next mirror snapshot is incomplete, waiting: "
                 << next_remote_snap->id << dendl;
        return; // wait and try later
      } else {
        can_proceed = true; // we have a complete mirror snapshot
        break;
      }
    }

    if (!can_proceed) {
      dout(10) << "no valid mirror snapshot found after: " << snap.id << dendl;
      return;
    }

    dout(10) << "found regular snap, snap name: " << snap.name
             << ", remote group snap id: " << snap.id << dendl;

    m_in_flight_op_tracker.start_op();
    auto ctx = new LambdaContext([this, snap](int r) mutable {
      if (r < 0) {
        dout(10) << "create regular snapshot failed, will be retried later: "
             << cpp_strerror(r) << dendl;
      }
      m_in_flight_op_tracker.finish_op();
    });
    create_regular_snapshot(&snap, ctx);
  }
}

template <typename I>
void Replayer<I>::update_local_group_state(cls::rbd::GroupSnapshot snap) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  m_in_flight_op_tracker.start_op();
  auto ctx = new LambdaContext([this, snap](int r) mutable {
    handle_update_local_group_state(r, std::move(snap));
    m_in_flight_op_tracker.finish_op();
  });

  // Set the mirror group state to enabled after the first non-primary
  // mirror snapshot is created
  auto req = GroupMirrorStateUpdateRequest<I>::create(m_local_io_ctx,
                                                      m_local_group_id,
                                                      m_image_replayers->size(),
                                                      ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_update_local_group_state(int r,
                                                  cls::rbd::GroupSnapshot snap) {
  std::unique_lock locker{m_lock};
  dout(10) << dendl;
  if (r < 0) {
    derr << "failed to set group state: " << cpp_strerror(r) << dendl;
    handle_replay_complete(&locker, r, "failed to set group state to enabled");
    return;
  }

  m_update_group_state = false;

  set_image_replayer_limits("", &snap, &locker);
}

template <typename I>
void Replayer<I>::create_mirror_snapshot(
    cls::rbd::GroupSnapshot *snap,
    const cls::rbd::MirrorSnapshotState &snap_state,
    Context *on_finish) {
  auto group_snap_id = snap->id;
  dout(10) << group_snap_id << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == group_snap_id;
      });

  if (itl != m_local_group_snaps.end() &&
      itl->state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
    dout(20) << "group snapshot: " << group_snap_id << " already exists"
             << dendl;
    on_finish->complete(0);
    return;
  }

  int r;
  std::set<std::string> mirror_peer_uuids;
  if (snap_state == cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED) {
    librados::IoCtx default_ns_io_ctx;
    default_ns_io_ctx.dup(m_local_io_ctx);

    default_ns_io_ctx.set_namespace("");
    std::vector<cls::rbd::MirrorPeer> mirror_peers;
    r = librbd::cls_client::mirror_peer_list(&default_ns_io_ctx, &mirror_peers);
    if (r < 0) {
      derr << "failed to list mirror peers: " << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }

    for (auto &peer : mirror_peers) {
      if (peer.mirror_peer_direction == cls::rbd::MIRROR_PEER_DIRECTION_RX) {
        continue;
      }
      mirror_peer_uuids.insert(peer.uuid);
    }
  }

  cls::rbd::GroupSnapshot local_snap =
  {group_snap_id,
    cls::rbd::GroupSnapshotNamespaceMirror{
      snap_state, mirror_peer_uuids, m_remote_mirror_uuid, group_snap_id},
    {}, cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  local_snap.name = prepare_non_primary_mirror_snap_name(m_global_group_id,
                                                         group_snap_id);

  auto comp = create_rados_callback(
      new LambdaContext([this, group_snap_id, on_finish](int r) {
        handle_create_mirror_snapshot(r, group_snap_id, on_finish);
        }));

  librados::ObjectWriteOperation op;
  librbd::cls_client::group_snap_set(&op, local_snap);
  r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void Replayer<I>::handle_create_mirror_snapshot(
    int r, const std::string &group_snap_id, Context *on_finish) {
  dout(10) << "group_snap_id=" << group_snap_id << ", r=" << r << dendl;

  if (r < 0) {
    derr << "failed to create mirror snapshot: " << group_snap_id
         << ", error: " << cpp_strerror(r) << dendl;
  }

  on_finish->complete(r);
}

template <typename I>
void Replayer<I>::mirror_snapshot_complete(
    const std::string &group_snap_id, Context *on_finish) {
  std::unique_lock locker{m_lock};
  dout(10) << group_snap_id << dendl;

  // find local snapshot
  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [&group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == group_snap_id;
      });

  if (itl == m_local_group_snaps.end()) {
    locker.unlock();
    on_finish->complete(0);
    return;
  }

  // find remote snapshot
  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [&group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == group_snap_id;
      });

  if (itr == m_remote_group_snaps.end()) {
    derr << "remote group snapshot doesn't exist: " << group_snap_id << dendl;
    locker.unlock();
    on_finish->complete(-ENOENT);
    return;
  }

  // copy snapshots before releasing lock
  cls::rbd::GroupSnapshot local_snap = *itl;
  cls::rbd::GroupSnapshot remote_snap = *itr;
  locker.unlock();

  bufferlist* out_bl = new bufferlist();
  std::vector<cls::rbd::GroupImageStatus>* local_images =
    new std::vector<cls::rbd::GroupImageStatus>();

  auto ctx = new LambdaContext(
    [this, group_snap_id,
     local_snap, remote_snap,  // captured by value (const in lambda)
     local_images, out_bl, on_finish](int r) {
      if (r < 0) {
        derr << "failed to list local group images: " << cpp_strerror(r) << dendl;
        delete local_images;
        delete out_bl;
        on_finish->complete(r);
        return;
      }

      // create cleanup context for next operation
      auto cleanup_ctx = new LambdaContext(
        [local_images, out_bl, on_finish](int r) {
          delete local_images;
          delete out_bl;
          on_finish->complete(r);
        });

      handle_mirror_snapshot_image_list(
        group_snap_id, local_snap, remote_snap,
        *local_images, cleanup_ctx);
    });

  // initiate image listing
  local_group_image_list_by_id(out_bl, local_images, ctx);
}

template <typename I>
void Replayer<I>::handle_mirror_snapshot_image_list(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish) {
  dout(10) << group_snap_id << dendl;
  post_mirror_snapshot_complete(group_snap_id, local_snap, remote_snap,
                                local_images, on_finish);
}

template <typename I>
void Replayer<I>::post_mirror_snapshot_complete(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish) {
  std::unique_lock locker{m_lock};
  dout(10) << group_snap_id << dendl;
  std::vector<cls::rbd::ImageSnapshotSpec> local_image_snap_specs;
  local_image_snap_specs.reserve(remote_snap.snaps.size());

  for (auto& image : local_images) {
    bool image_snap_complete = false;
    std::string image_header_oid = librbd::util::header_name(image.spec.image_id);
    ::SnapContext snapc;
    int r = librbd::cls_client::get_snapcontext(&m_local_io_ctx, image_header_oid, &snapc);
    if (r < 0) {
      derr << "get snap context failed: " << cpp_strerror(r) << dendl;
      locker.unlock();
      on_finish->complete(r);
      return;
    }

    // process snapshots in reverse order
    for (auto snap_id : snapc.snaps) {
      cls::rbd::SnapshotInfo snap_info;
      r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
          snap_id, &snap_info);
      if (r < 0) {
        derr << "failed getting snap info for snap id: " << snap_id
             << ", : " << cpp_strerror(r) << dendl;
        locker.unlock();
        on_finish->complete(r);
        return;
      }

      auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
          &snap_info.snapshot_namespace);
      if (!mirror_ns) {
        continue;
      }

      // make sure the image snapshot is COMPLETE
      if (mirror_ns->group_snap_id == group_snap_id && mirror_ns->complete) {
        image_snap_complete = true;
        cls::rbd::ImageSnapshotSpec snap_spec;
        snap_spec.pool = image.spec.pool_id;
        snap_spec.image_id = image.spec.image_id;
        snap_spec.snap_id = snap_info.id;

        // check if this spec already exists in local snaps
        auto it = std::find_if(local_snap.snaps.begin(), local_snap.snaps.end(),
          [&snap_spec](const cls::rbd::ImageSnapshotSpec &s) {
            return snap_spec.pool == s.pool &&
                   snap_spec.image_id == s.image_id;
          });
        if (it == local_snap.snaps.end()) {
          local_image_snap_specs.push_back(snap_spec);
        }
        continue;
      } else {
        dout(10) << "remote group snap id: " << group_snap_id
                 << ", local reflected in the image snap: "
                 << mirror_ns->group_snap_id << dendl;
      }
    }

    if (!image_snap_complete) {
      set_image_replayer_limits(image.spec.image_id, &remote_snap, &locker);
    }
  }

  if (remote_snap.snaps.size() == local_image_snap_specs.size()) {
    cls::rbd::GroupSnapshot local_snap_copy = local_snap;
    local_snap_copy.snaps = local_image_snap_specs;
    local_snap_copy.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;

    librados::ObjectWriteOperation op;
    librbd::cls_client::group_snap_set(&op, local_snap_copy);

    auto comp = create_rados_callback(
      new LambdaContext([this, group_snap_id, on_finish](int r) {
        handle_post_mirror_snapshot_complete(r, group_snap_id, on_finish);
      }));
    int r = m_local_io_ctx.aio_operate(
        librbd::util::group_header_name(m_local_group_id), comp, &op);
    ceph_assert(r == 0);
    comp->release();

    dout(10) << "local group snap info: "
             << "id: " << local_snap_copy.id
             << ", name: " << local_snap_copy.name
             << ", state: " << local_snap_copy.state
             << ", snaps.size: " << local_snap_copy.snaps.size()
             << dendl;
  } else {
    locker.unlock();
    on_finish->complete(0);
  }
}

template <typename I>
void Replayer<I>::handle_post_mirror_snapshot_complete(
    int r, const std::string &group_snap_id, Context *on_finish) {
  dout(10) << group_snap_id << ", r=" << r << dendl;

  if (r < 0) {
    on_finish->complete(r);
    return;
  }

  {
    std::unique_lock locker{m_lock};
    utime_t duration = ceph_clock_now() - m_snapshot_start;
    m_last_snapshot_complete_seconds = duration.sec();
    m_snapshot_start = utime_t(0, 0);
  }

  uint64_t last_snapshot_bytes = 0;
  for (const auto& ir : *m_image_replayers) {
    if (ir.second != nullptr) {
      last_snapshot_bytes += ir.second->get_last_snapshot_bytes();
    }
  }

  {
    std::unique_lock locker{m_lock};
    m_last_snapshot_bytes = last_snapshot_bytes;
  }

  on_finish->complete(0);
}

template <typename I>
void Replayer<I>::create_regular_snapshot(
    cls::rbd::GroupSnapshot *snap,
    Context *on_finish) {
  auto group_snap_id = snap->id;
  dout(10) << group_snap_id << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  // check if snapshot already exists
  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [group_snap_id](const cls::rbd::GroupSnapshot &s) {
        return s.id == group_snap_id;
      });

  if (itl != m_local_group_snaps.end() &&
      itl->state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
    dout(20) << "group snapshot: " << group_snap_id << " already exists"
             << dendl;
    on_finish->complete(0);
    return;
  }

  librados::ObjectWriteOperation op;
  cls::rbd::GroupSnapshot group_snap{
    group_snap_id, // keeping it same as remote group snap id
    cls::rbd::GroupSnapshotNamespaceUser{},
    snap->name,
    cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};

  librbd::cls_client::group_snap_set(&op, group_snap);

  auto comp = create_rados_callback(
      new LambdaContext([this, group_snap_id, on_finish](int r) {
        handle_create_regular_snapshot(r, group_snap_id, on_finish);
      }));

  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void Replayer<I>::handle_create_regular_snapshot(
    int r, const std::string &group_snap_id, Context *on_finish) {
  dout(10) << "group_snap_id=" << group_snap_id << ", r=" << r << dendl;

  if (r < 0) {
    derr << "failed to create regular snapshot: " << group_snap_id
         << ", error: " << cpp_strerror(r) << dendl;
  }

  on_finish->complete(r);
}

template <typename I>
void Replayer<I>::regular_snapshot_complete(
    const std::string &group_snap_id,
    Context *on_finish) {
  std::unique_lock locker{m_lock};
  dout(10) << group_snap_id << dendl;

  // find local snapshot
  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [&group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == group_snap_id;
      });

  if (itl == m_local_group_snaps.end()) {
    locker.unlock();
    on_finish->complete(0);
    return;
  }

  // find remote snapshot
  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [&group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == group_snap_id;
      });

  if (itr == m_remote_group_snaps.end()) {
    derr << "remote group snapshot doesn't exist: " << group_snap_id << dendl;
    locker.unlock();
    on_finish->complete(-ENOENT);
    return;
  }

  // copy snapshots before releasing lock
  cls::rbd::GroupSnapshot local_snap = *itl;
  cls::rbd::GroupSnapshot remote_snap = *itr;
  locker.unlock();

  bufferlist* out_bl = new bufferlist();
  std::vector<cls::rbd::GroupImageStatus>* local_images =
    new std::vector<cls::rbd::GroupImageStatus>();

  auto ctx = new LambdaContext(
    [this, group_snap_id,
     local_snap, remote_snap,  // captured by value (const in lambda)
     local_images, out_bl, on_finish](int r) {
      if (r < 0) {
        derr << "failed to list group images: " << cpp_strerror(r) << dendl;
        delete local_images;
        delete out_bl;
        on_finish->complete(r);
        return;
      }

      // create cleanup context for next operation
      auto cleanup_ctx = new LambdaContext(
        [local_images, out_bl, on_finish](int r) {
          delete local_images;
          delete out_bl;
          on_finish->complete(r);
        });

      handle_regular_snapshot_image_list(
        group_snap_id, local_snap, remote_snap,
        *local_images, cleanup_ctx);
    });

  // initiate image listing
  local_group_image_list_by_id(out_bl, local_images, ctx);
}

template <typename I>
void Replayer<I>::handle_regular_snapshot_image_list(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish) {
  dout(10) << group_snap_id << dendl;
  post_regular_snapshot_complete(group_snap_id, local_snap, remote_snap,
                                 local_images, on_finish);
}

template <typename I>
void Replayer<I>::post_regular_snapshot_complete(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish) {
  std::unique_lock locker{m_lock};
  dout(10) << group_snap_id << dendl;

  // each image will have one snapshot specific to group snap, and so for each
  // image get a ImageSnapshotSpec and prepare a vector
  // for image :: <images in that group> {
  //   * get snap whos name has group snap_id for that we can list snaps and
  //     filter with remote_group_snap_id
  //   * get its { pool_id, snap_id, image_id }
  // }
  // finally write to the object

  std::vector<cls::rbd::ImageSnapshotSpec> local_image_snap_specs;
  local_image_snap_specs.reserve(remote_snap.snaps.size());

  for (auto &image : local_images) {
    std::string image_header_oid = librbd::util::header_name(
        image.spec.image_id);
    ::SnapContext snapc;
    int r = librbd::cls_client::get_snapcontext(&m_local_io_ctx,
        image_header_oid, &snapc);
    if (r < 0) {
      derr << "get snap context failed: " << cpp_strerror(r) << dendl;
      locker.unlock();
      on_finish->complete(r);
      return;
    }

    for (auto snap_id : snapc.snaps) {
      cls::rbd::SnapshotInfo snap_info;
      r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
          snap_id, &snap_info);
      if (r < 0) {
        derr << "failed getting snap info for snap id: " << snap_id
             << ", : " << cpp_strerror(r) << dendl;
        locker.unlock();
        on_finish->complete(r);
        return;
      }

      // extract { pool_id, snap_id, image_id }
      auto ns = std::get_if<cls::rbd::ImageSnapshotNamespaceGroup>(
          &snap_info.snapshot_namespace);
      if (ns != nullptr && ns->group_snapshot_id == group_snap_id) {
        cls::rbd::ImageSnapshotSpec snap_spec;
        snap_spec.pool = image.spec.pool_id;
        snap_spec.image_id = image.spec.image_id;
        snap_spec.snap_id = snap_info.id;

        local_image_snap_specs.push_back(snap_spec);
      }
    }
  }

  if (remote_snap.snaps.size() == local_image_snap_specs.size()) {
    cls::rbd::GroupSnapshot local_snap_copy = local_snap;
    local_snap_copy.snaps = local_image_snap_specs;
    local_snap_copy.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;

    librados::ObjectWriteOperation op;
    librbd::cls_client::group_snap_set(&op, local_snap_copy);

    auto comp = create_rados_callback(
        new LambdaContext([this, group_snap_id, on_finish](int r) {
          handle_post_regular_snapshot_complete(r, group_snap_id, on_finish);
        }));
    int r = m_local_io_ctx.aio_operate(
        librbd::util::group_header_name(m_local_group_id), comp, &op);
    ceph_assert(r == 0);
    comp->release();

    dout(10) << "local group snap info: "
             << "id: " << local_snap_copy.id
             << ", name: " << local_snap_copy.name
             << ", state: " << local_snap_copy.state
             << ", snaps.size: " << local_snap_copy.snaps.size()
             << dendl;
  } else {
    locker.unlock();
    on_finish->complete(0);
  }
}

template <typename I>
void Replayer<I>::handle_post_regular_snapshot_complete(
    int r, const std::string &group_snap_id, Context *on_finish) {
  dout(10) << group_snap_id << ", r=" << r << dendl;

  on_finish->complete(r);
}

template <typename I>
void Replayer<I>::mirror_group_snapshot_unlink_peer(const std::string &snap_id) {
  auto remote_snap = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == snap_id;
      });

  if (remote_snap == m_remote_group_snaps.end()) {
    derr << "remote group snapshot not found: "
         << snap_id << dendl;
    return;
  }

  auto rns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &remote_snap->snapshot_namespace);
  if (rns == nullptr) {
    derr << "remote group snapshot is not a mirror snapshot: "
         << snap_id << dendl;
    return;
  }

  if (rns->mirror_peer_uuids.count(m_remote_mirror_peer_uuid) != 0) {
    rns->mirror_peer_uuids.erase(m_remote_mirror_peer_uuid);
    auto comp = create_rados_callback(
      new LambdaContext([this, snap_id](int r) {
	handle_mirror_group_snapshot_unlink_peer(r, snap_id);
      }));

    librados::ObjectWriteOperation op;
    librbd::cls_client::group_snap_set(&op, *remote_snap);
    int r = m_remote_io_ctx.aio_operate(
      librbd::util::group_header_name(m_remote_group_id), comp, &op);
    ceph_assert(r == 0);
    comp->release();
  }
}

template <typename I>
void Replayer<I>::handle_mirror_group_snapshot_unlink_peer(
    int r, const std::string &snap_id) {
  dout(10) << snap_id << ", r=" << r << dendl;

  if (r < 0) {
    derr << "failed to remove mirror_peer_uuid for snap_id: "
         << snap_id  << " : " << cpp_strerror(r) << dendl;
  }

  return;
}

template <typename I>
bool Replayer<I>::prune_all_image_snapshots(
    cls::rbd::GroupSnapshot *local_snap,
    std::unique_lock<ceph::mutex>* locker) {
  bool retain = false;

  if (!local_snap) {
    return retain;
  }

  dout(10) << "attempting to prune image snaps from group snap: "
    << local_snap->id << dendl;

  for (auto &spec : local_snap->snaps) {
    std::string image_header_oid = librbd::util::header_name(spec.image_id);
    cls::rbd::SnapshotInfo snap_info;
    int r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
        spec.snap_id, &snap_info);
    if (r == -ENOENT) {
      continue;
    } else if (r < 0) {
      derr << "failed getting snap info for snap id: " << spec.snap_id
        << ", : " << cpp_strerror(r) << dendl;
    }
    retain = true;
    locker->unlock();
    for (auto it = m_image_replayers->begin();
        it != m_image_replayers->end(); ++it) {
      auto image_replayer = it->second;
      if (!image_replayer) {
        continue;
      }
      auto local_image_id = image_replayer->get_local_image_id();
      if (local_image_id.empty()) {
        continue;
      }
      if (local_image_id != spec.image_id) {
        continue;
      }
      dout(10) << "pruning: " << spec.snap_id << dendl;
      // The ImageReplayer can have m_replayer empty, so no guaranty that
      // this will succeed in one shot, but we keep retry for this and
      // acheive anyway.
      image_replayer->prune_snapshot(spec.snap_id);
      break;
    }
    locker->lock();
  }

  return retain;
}

template <typename I>
void Replayer<I>::prune_user_group_snapshots(
    std::unique_lock<ceph::mutex>* locker) {
  int r;
  for (auto local_snap = m_local_group_snaps.begin();
      local_snap != m_local_group_snaps.end(); ++local_snap) {
    if (local_snap->state != cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
      break;
    }
    auto snap_type = cls::rbd::get_group_snap_namespace_type(
        local_snap->snapshot_namespace);
    if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
      bool prune_user_snap = true;
      for (auto &remote_snap : m_remote_group_snaps) {
        if (remote_snap.name == local_snap->name) {
          prune_user_snap = false;
          break;
        }
      }
      if (!prune_user_snap) {
        continue;
      }
      dout(10) << "pruning regular group snap in-progress: "
               << local_snap->name << ", with id: " << local_snap->id << dendl;
      // prune all the image snaps of the group snap locally
      if (prune_all_image_snapshots(&(*local_snap), locker)) {
        continue;
      }
      dout(10) << "all image snaps are pruned, finally pruning regular group snap: "
               << local_snap->id << dendl;
      r = librbd::cls_client::group_snap_remove(&m_local_io_ctx,
             librbd::util::group_header_name(m_local_group_id), local_snap->id);
      if (r < 0) {
        derr << "failed to remove group snapshot : "
             << local_snap->id << " : " << cpp_strerror(r) << dendl;
      }
    }
  }
}

template <typename I>
void Replayer<I>::prune_mirror_group_snapshots(
    std::unique_lock<ceph::mutex>* locker) {
  int r;
  bool is_prior_snap_user = false;
  bool skip_next_snap_check = false;
  cls::rbd::GroupSnapshot *prune_snap = nullptr;
  auto last_local_snap_id = m_local_group_snaps.rbegin()->id;
  for (auto local_snap = m_local_group_snaps.begin();
      local_snap != m_local_group_snaps.end(); ++local_snap) {
    if (local_snap->state != cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
      break;
    }

    auto snap_type = cls::rbd::get_group_snap_namespace_type(
        local_snap->snapshot_namespace);
    if (snap_type != cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
      is_prior_snap_user = true;
      if (prune_snap) { // snapshot before user snap exists ?
        // delete snap before user snap, only if the user snap has next complete mirror snap
        auto next_local_snap = std::next(local_snap);
        if (next_local_snap == m_local_group_snaps.end()) {
          break; // no next snap.
        }
        auto next_snap_type = cls::rbd::get_group_snap_namespace_type(
            next_local_snap->snapshot_namespace);
        if (next_snap_type != cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
          continue; // next snap is user snap again.
        } else if (next_local_snap->state != cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
          break; // next snap is not complete yet.
        }
      }
      continue;
    }
    if (!prune_snap) {
      prune_snap = &(*local_snap);
    }

    if (is_prior_snap_user) {
      is_prior_snap_user = false; // free to continue prune on next iteratation
      skip_next_snap_check = true; // as we are pruning mirror snap before user snap
      continue;
    }

    if (!skip_next_snap_check) {
      auto next_local_snap = std::next(local_snap);
      // If next local snap is end, or if it is the syncing in-progress snap,
      // then we still need this group snapshot.
      if (next_local_snap == m_local_group_snaps.end() ||
          (next_local_snap->id == last_local_snap_id &&
           next_local_snap->state != cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE)) {
        break;
      } else {
        auto next_snap_type = cls::rbd::get_group_snap_namespace_type(
            next_local_snap->snapshot_namespace);
        if (next_snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
          continue;
        }
      }
    }
    mirror_group_snapshot_unlink_peer(prune_snap->id);
    // prune all the image snaps of the group snap locally
    if (prune_all_image_snapshots(prune_snap, locker)) {
      prune_snap = nullptr;
      skip_next_snap_check = false;
      continue;
    }
    dout(10) << "all image snaps are pruned, finally pruning mirror group snap: "
             << prune_snap->id << dendl;
    r = librbd::cls_client::group_snap_remove(&m_local_io_ctx,
        librbd::util::group_header_name(m_local_group_id), prune_snap->id);
    if (r < 0) {
      derr << "failed to remove group snapshot : "
           << prune_snap->id << " : " << cpp_strerror(r) << dendl;
    }
    prune_snap = nullptr;
    skip_next_snap_check = false;
  }
}

template <typename I>
void Replayer<I>::prune_group_snapshots(std::unique_lock<ceph::mutex>* locker) {
  if (m_local_group_snaps.empty()) {
    return;
  }
  prune_user_group_snapshots(locker);
  prune_mirror_group_snapshots(locker);
}

// set_image_replayer_limits, sets limits of remote_snap_id_end in the image
// replayer for all the respective images part of a remote group snapshot.
// If image_id is specified it will only set for image replayer belonging to
// that image_id only, if the image_id is empty it sets for all matching image
// replayers.
template <typename I>
void Replayer<I>::set_image_replayer_limits(const std::string &image_id,
    const cls::rbd::GroupSnapshot *remote_snap,
  std::unique_lock<ceph::mutex>* locker) {
  if (!remote_snap) {
    return;
  }

  locker->unlock();
  for (auto it = m_image_replayers->begin();
      it != m_image_replayers->end(); ++it) {
    auto image_replayer = it->second;
    if (!image_replayer) {
      continue;
    }
    auto local_image_id = image_replayer->get_local_image_id();
    if (!local_image_id.empty() && local_image_id != image_id) {
      continue;
    }
    auto global_image_id = image_replayer->get_global_image_id();
    if (global_image_id.empty()) {
      derr << "global_image_id is empty for: " << image_id << dendl;
      break;
    }
    for (auto &spec : remote_snap->snaps) {
      cls::rbd::MirrorImage mirror_image;
      int r = librbd::cls_client::mirror_image_get(&m_remote_io_ctx,
          spec.image_id, &mirror_image);
      if (r < 0) {
        derr << "mirror image get failed for: " << spec.image_id << " : "
          << cpp_strerror(r) << dendl;
        continue;
      }
      if (global_image_id != mirror_image.global_image_id) {
        continue;
      }
      std::string image_header_oid = librbd::util::header_name(spec.image_id);
      cls::rbd::SnapshotInfo snap_info;
      r = librbd::cls_client::snapshot_get(&m_remote_io_ctx, image_header_oid,
          spec.snap_id, &snap_info);
      if (r < 0) {
        derr << "failed getting snap info for snap id: " << spec.snap_id
             << ", : " << cpp_strerror(r) << dendl;
        continue;
      }
      auto remote_snap_id_end = image_replayer->get_remote_snap_id_end_limit();
      if (remote_snap_id_end == CEPH_NOSNAP || remote_snap_id_end < snap_info.id) {
        image_replayer->set_remote_snap_id_end_limit(snap_info.id);
      }
      break;
    }
  }
  locker->lock();
}

template <typename I>
void Replayer<I>::shut_down(Context* on_finish) {
  dout(10) << dendl;

  {
    std::unique_lock locker{m_lock};
    m_stop_requested = false;
    ceph_assert(m_on_shutdown == nullptr);
    std::swap(m_on_shutdown, on_finish);

    auto state = STATE_COMPLETE;
    std::swap(m_state, state);
  }

  cancel_load_group_snapshots();

  if (!m_in_flight_op_tracker.empty()) {
    m_in_flight_op_tracker.wait_for_ops(new LambdaContext([this](int) {
        finish_shut_down();
      }));
    return;
  }

  finish_shut_down();
  return;
}

template <typename I>
void Replayer<I>::finish_shut_down() {
  dout(10) << dendl;

  Context *on_finish = nullptr;

  {
    std::unique_lock locker{m_lock};
    ceph_assert(m_on_shutdown != nullptr);
    std::swap(m_on_shutdown, on_finish);
  }
  if (on_finish) {
    on_finish->complete(0);
  }
}

template <typename I>
bool Replayer<I>::get_replay_status(std::string* description) {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  if (m_state != STATE_REPLAYING) {
    derr << "replay not running" << dendl;
    return false;
  }

  json_spirit::mObject root_obj;
  root_obj["last_snapshot_complete_seconds"] =
    m_last_snapshot_complete_seconds;
  root_obj["last_snapshot_bytes"] = m_last_snapshot_bytes;

  auto remote_gp_snap_ptr = get_latest_complete_mirror_group_snapshot(
    m_remote_group_snaps);
  if (remote_gp_snap_ptr != nullptr) {
    utime_t timestamp;
    int r = get_group_snapshot_timestamp(m_remote_io_ctx, *remote_gp_snap_ptr,
                                         &timestamp);
    if (r < 0) {
      derr << "error getting timestamp of remote group snapshot ID: "
           << remote_gp_snap_ptr->id << ", r=" << cpp_strerror(r) << dendl;
      return false;
    }
    root_obj["remote_snapshot_timestamp"] = timestamp.sec();
  } else {
    return false;
  }

  auto latest_local_gp_snap_ptr = get_latest_complete_mirror_group_snapshot(
    m_local_group_snaps);
  if (latest_local_gp_snap_ptr != nullptr) {
    // find remote group snap with ID matching that of the latest local
    // group snap
    remote_gp_snap_ptr = nullptr;
    for (const auto& remote_group_snap: m_remote_group_snaps) {
      if (remote_group_snap.id == latest_local_gp_snap_ptr->id) {
	remote_gp_snap_ptr = &remote_group_snap;
	break;
      }
    }
    if (remote_gp_snap_ptr != nullptr) {
      utime_t timestamp;
      int r = get_group_snapshot_timestamp(m_remote_io_ctx,
                                           *remote_gp_snap_ptr,
                                           &timestamp);
      if (r < 0) {
	derr << "error getting timestamp of matching remote group snapshot ID: "
             << remote_gp_snap_ptr->id << ", r=" << cpp_strerror(r) << dendl;
      } else {
	root_obj["local_snapshot_timestamp"] = timestamp.sec();
      }
    }
  }

  *description = json_spirit::write(root_obj,
                                    json_spirit::remove_trailing_zeros);
  return true;
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::Replayer<librbd::ImageCtx>;
