// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Replayer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "common/Timer.h"
#include "cls/rbd/cls_rbd_client.h"
#include "json_spirit/json_spirit.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/deep_copy/Handler.h"
#include "librbd/deep_copy/ImageCopyRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.h"
#include "librbd/mirror/snapshot/GetImageStateRequest.h"
#include "librbd/mirror/snapshot/ImageMeta.h"
#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/ReplayerListener.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "tools/rbd_mirror/image_replayer/snapshot/ApplyImageStateRequest.h"
#include "tools/rbd_mirror/image_replayer/snapshot/StateBuilder.h"
#include "tools/rbd_mirror/image_replayer/snapshot/Utils.h"
#include <set>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::snapshot::" \
                           << "Replayer: " << this << " " << __func__ << ": "

extern PerfCounters *g_perf_counters;

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

namespace {

double round_to_two_places(double value) {
  return abs(round(value * 100) / 100);
}

template<typename I>
std::pair<uint64_t, librbd::SnapInfo*> get_newest_mirror_snapshot(
    I* image_ctx) {
  for (auto snap_info_it = image_ctx->snap_info.rbegin();
       snap_info_it != image_ctx->snap_info.rend(); ++snap_info_it) {
    const auto& snap_ns = snap_info_it->second.snap_namespace;
    auto mirror_ns = boost::get<
      cls::rbd::MirrorSnapshotNamespace>(&snap_ns);
    if (mirror_ns == nullptr || !mirror_ns->complete) {
      continue;
    }

    return {snap_info_it->first, &snap_info_it->second};
  }

  return {CEPH_NOSNAP, nullptr};
}

} // anonymous namespace

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
struct Replayer<I>::C_UpdateWatchCtx : public librbd::UpdateWatchCtx {
  Replayer<I>* replayer;

  C_UpdateWatchCtx(Replayer<I>* replayer) : replayer(replayer) {
  }

  void handle_notify() override {
     replayer->handle_image_update_notify();
  }
};

template <typename I>
struct Replayer<I>::DeepCopyHandler : public librbd::deep_copy::Handler {
  Replayer *replayer;

  DeepCopyHandler(Replayer* replayer) : replayer(replayer) {
  }

  void handle_read(uint64_t bytes_read) override {
    replayer->handle_copy_image_read(bytes_read);
  }

  int update_progress(uint64_t object_number, uint64_t object_count) override {
    replayer->handle_copy_image_progress(object_number, object_count);
    return 0;
  }
};

template <typename I>
Replayer<I>::Replayer(
    Threads<I>* threads,
    InstanceWatcher<I>* instance_watcher,
    const std::string& local_mirror_uuid,
    PoolMetaCache* pool_meta_cache,
    StateBuilder<I>* state_builder,
    ReplayerListener* replayer_listener)
  : m_threads(threads),
    m_instance_watcher(instance_watcher),
    m_local_mirror_uuid(local_mirror_uuid),
    m_pool_meta_cache(pool_meta_cache),
    m_state_builder(state_builder),
    m_replayer_listener(replayer_listener),
    m_lock(ceph::make_mutex(librbd::util::unique_lock_name(
      "rbd::mirror::image_replayer::snapshot::Replayer", this))) {
  dout(10) << dendl;
}

template <typename I>
Replayer<I>::~Replayer() {
  dout(10) << dendl;
  ceph_assert(m_state == STATE_COMPLETE);
  ceph_assert(m_update_watch_ctx == nullptr);
  ceph_assert(m_deep_copy_handler == nullptr);
}

template <typename I>
void Replayer<I>::init(Context* on_finish) {
  dout(10) << dendl;

  ceph_assert(m_state == STATE_INIT);

  RemotePoolMeta remote_pool_meta;
  int r = m_pool_meta_cache->get_remote_pool_meta(
    m_state_builder->remote_image_ctx->md_ctx.get_id(), &remote_pool_meta);
  if (r < 0 || remote_pool_meta.mirror_peer_uuid.empty()) {
    derr << "failed to retrieve mirror peer uuid from remote pool" << dendl;
    m_state = STATE_COMPLETE;
    m_threads->work_queue->queue(on_finish, r);
    return;
  }

  m_remote_mirror_peer_uuid = remote_pool_meta.mirror_peer_uuid;
  dout(10) << "remote_mirror_peer_uuid=" << m_remote_mirror_peer_uuid << dendl;

  ceph_assert(m_on_init_shutdown == nullptr);
  m_on_init_shutdown = on_finish;

  register_local_update_watcher();
}

template <typename I>
void Replayer<I>::shut_down(Context* on_finish) {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  ceph_assert(m_on_init_shutdown == nullptr);
  m_on_init_shutdown = on_finish;
  m_error_code = 0;
  m_error_description = "";

  ceph_assert(m_state != STATE_INIT);
  auto state = STATE_COMPLETE;
  std::swap(m_state, state);

  if (state == STATE_REPLAYING) {
    // if a sync request was pending, request a cancelation
    m_instance_watcher->cancel_sync_request(
      m_state_builder->local_image_ctx->id);

    // TODO interrupt snapshot copy and image copy state machines even if remote
    // cluster is unreachable
    dout(10) << "shut down pending on completion of snapshot replay" << dendl;
    return;
  }
  locker.unlock();

  unregister_remote_update_watcher();
}

template <typename I>
void Replayer<I>::flush(Context* on_finish) {
  dout(10) << dendl;

  // TODO
  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
bool Replayer<I>::get_replay_status(std::string* description,
                                    Context* on_finish) {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  if (m_state != STATE_REPLAYING && m_state != STATE_IDLE) {
    locker.unlock();

    derr << "replay not running" << dendl;
    on_finish->complete(-EAGAIN);
    return false;
  }

  std::shared_lock local_image_locker{
    m_state_builder->local_image_ctx->image_lock};
  auto [local_snap_id, local_snap_info] = get_newest_mirror_snapshot(
    m_state_builder->local_image_ctx);

  std::shared_lock remote_image_locker{
    m_state_builder->remote_image_ctx->image_lock};
  auto [remote_snap_id, remote_snap_info] = get_newest_mirror_snapshot(
    m_state_builder->remote_image_ctx);

  if (remote_snap_info == nullptr) {
    remote_image_locker.unlock();
    local_image_locker.unlock();
    locker.unlock();

    derr << "remote image does not contain mirror snapshots" << dendl;
    on_finish->complete(-EAGAIN);
    return false;
  }

  std::string replay_state = "idle";
  if (m_remote_snap_id_end != CEPH_NOSNAP) {
    replay_state = "syncing";
  }

  json_spirit::mObject root_obj;
  root_obj["replay_state"] = replay_state;
  root_obj["remote_snapshot_timestamp"] = remote_snap_info->timestamp.sec();

  auto matching_remote_snap_id = util::compute_remote_snap_id(
    m_state_builder->local_image_ctx->image_lock,
    m_state_builder->local_image_ctx->snap_info,
    local_snap_id, m_state_builder->remote_mirror_uuid);
  auto matching_remote_snap_it =
    m_state_builder->remote_image_ctx->snap_info.find(matching_remote_snap_id);
  if (matching_remote_snap_id != CEPH_NOSNAP &&
      matching_remote_snap_it !=
        m_state_builder->remote_image_ctx->snap_info.end()) {
    // use the timestamp from the matching remote image since
    // the local snapshot would just be the time the snapshot was
    // synced and not the consistency point in time.
    root_obj["local_snapshot_timestamp"] =
      matching_remote_snap_it->second.timestamp.sec();
  }

  matching_remote_snap_it = m_state_builder->remote_image_ctx->snap_info.find(
    m_remote_snap_id_end);
  if (m_remote_snap_id_end != CEPH_NOSNAP &&
      matching_remote_snap_it !=
        m_state_builder->remote_image_ctx->snap_info.end()) {
    root_obj["syncing_snapshot_timestamp"] = remote_snap_info->timestamp.sec();
    root_obj["syncing_percent"] = static_cast<uint64_t>(
        100 * m_local_mirror_snap_ns.last_copied_object_number /
        static_cast<float>(std::max<uint64_t>(1U, m_local_object_count)));
  }

  m_bytes_per_second(0);
  auto bytes_per_second = m_bytes_per_second.get_average();
  root_obj["bytes_per_second"] = round_to_two_places(bytes_per_second);

  auto bytes_per_snapshot = boost::accumulators::rolling_mean(
    m_bytes_per_snapshot);
  root_obj["bytes_per_snapshot"] = round_to_two_places(bytes_per_snapshot);

  auto pending_bytes = bytes_per_snapshot * m_pending_snapshots;
  if (bytes_per_second > 0 && m_pending_snapshots > 0) {
    auto seconds_until_synced = round_to_two_places(
      pending_bytes / bytes_per_second);
    if (seconds_until_synced >= std::numeric_limits<uint64_t>::max()) {
      seconds_until_synced = std::numeric_limits<uint64_t>::max();
    }

    root_obj["seconds_until_synced"] = static_cast<uint64_t>(
      seconds_until_synced);
  }

  *description = json_spirit::write(
    root_obj, json_spirit::remove_trailing_zeros);

  local_image_locker.unlock();
  remote_image_locker.unlock();
  locker.unlock();
  on_finish->complete(-EEXIST);
  return true;
}

template <typename I>
void Replayer<I>::load_local_image_meta() {
  dout(10) << dendl;

  {
    // reset state in case new snapshot is added while we are scanning
    std::unique_lock locker{m_lock};
    m_image_updated = false;
  }

  ceph_assert(m_state_builder->local_image_meta != nullptr);
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_load_local_image_meta>(this);
  m_state_builder->local_image_meta->load(ctx);
}

template <typename I>
void Replayer<I>::handle_load_local_image_meta(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "failed to load local image-meta: " << cpp_strerror(r) << dendl;
    handle_replay_complete(r, "failed to load local image-meta");
    return;
  }

  if (r >= 0 && m_state_builder->local_image_meta->resync_requested) {
    m_resync_requested = true;

    dout(10) << "local image resync requested" << dendl;
    handle_replay_complete(0, "resync requested");
    return;
  }

  refresh_local_image();
}

template <typename I>
void Replayer<I>::refresh_local_image() {
  if (!m_state_builder->local_image_ctx->state->is_refresh_required()) {
    refresh_remote_image();
    return;
  }

  dout(10) << dendl;
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_refresh_local_image>(this);
  m_state_builder->local_image_ctx->state->refresh(ctx);
}

template <typename I>
void Replayer<I>::handle_refresh_local_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to refresh local image: " << cpp_strerror(r) << dendl;
    handle_replay_complete(r, "failed to refresh local image");
    return;
  }

  refresh_remote_image();
}

template <typename I>
void Replayer<I>::refresh_remote_image() {
  if (!m_state_builder->remote_image_ctx->state->is_refresh_required()) {
    std::unique_lock locker{m_lock};
    scan_local_mirror_snapshots(&locker);
    return;
  }

  dout(10) << dendl;
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_refresh_remote_image>(this);
  m_state_builder->remote_image_ctx->state->refresh(ctx);
}

template <typename I>
void Replayer<I>::handle_refresh_remote_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to refresh remote image: " << cpp_strerror(r) << dendl;
    handle_replay_complete(r, "failed to refresh remote image");
    return;
  }

  std::unique_lock locker{m_lock};
  scan_local_mirror_snapshots(&locker);
}

template <typename I>
void Replayer<I>::scan_local_mirror_snapshots(
    std::unique_lock<ceph::mutex>* locker) {
  if (is_replay_interrupted(locker)) {
    return;
  }

  dout(10) << dendl;

  m_local_snap_id_start = 0;
  m_local_snap_id_end = CEPH_NOSNAP;
  m_local_mirror_snap_ns = {};
  m_local_object_count = 0;

  m_remote_snap_id_start = 0;
  m_remote_snap_id_end = CEPH_NOSNAP;
  m_remote_mirror_snap_ns = {};

  std::set<uint64_t> prune_snap_ids;

  auto local_image_ctx = m_state_builder->local_image_ctx;
  std::shared_lock image_locker{local_image_ctx->image_lock};
  for (auto snap_info_it = local_image_ctx->snap_info.begin();
       snap_info_it != local_image_ctx->snap_info.end(); ++snap_info_it) {
    const auto& snap_ns = snap_info_it->second.snap_namespace;
    auto mirror_ns = boost::get<
      cls::rbd::MirrorSnapshotNamespace>(&snap_ns);
    if (mirror_ns == nullptr) {
      continue;
    }

    dout(15) << "local mirror snapshot: id=" << snap_info_it->first << ", "
             << "mirror_ns=" << *mirror_ns << dendl;
    m_local_mirror_snap_ns = *mirror_ns;

    auto local_snap_id = snap_info_it->first;
    if (mirror_ns->is_non_primary()) {
      if (mirror_ns->complete) {
        // if remote has new snapshots, we would sync from here
        m_local_snap_id_start = local_snap_id;
        m_local_snap_id_end = CEPH_NOSNAP;

        if (mirror_ns->mirror_peer_uuids.empty()) {
          // no other peer will attempt to sync to this snapshot so store as
          // a candidate for removal
          prune_snap_ids.insert(local_snap_id);
        }
      } else {
        // start snap will be last complete mirror snapshot or initial
        // image revision
        m_local_snap_id_end = local_snap_id;

        if (mirror_ns->last_copied_object_number == 0) {
          // snapshot might be missing image state, object-map, etc, so just
          // delete and re-create it if we haven't started copying data
          // objects
          prune_snap_ids.insert(local_snap_id);
          break;
        }
      }
    } else if (mirror_ns->is_primary()) {
      if (mirror_ns->complete) {
        m_local_snap_id_start = local_snap_id;
        m_local_snap_id_end = CEPH_NOSNAP;
      } else {
        derr << "incomplete local primary snapshot" << dendl;
        handle_replay_complete(locker, -EINVAL,
                               "incomplete local primary snapshot");
        return;
      }
    } else {
      derr << "unknown local mirror snapshot state" << dendl;
      handle_replay_complete(locker, -EINVAL,
                             "invalid local mirror snapshot state");
      return;
    }
  }
  image_locker.unlock();

  if (m_local_snap_id_start > 0 && m_local_snap_id_end == CEPH_NOSNAP) {
    // remove candidate that is required for delta snapshot sync
    prune_snap_ids.erase(m_local_snap_id_start);
  }
  if (!prune_snap_ids.empty()) {
    locker->unlock();

    auto prune_snap_id = *prune_snap_ids.begin();
    dout(5) << "pruning unused non-primary snapshot " << prune_snap_id << dendl;
    prune_non_primary_snapshot(prune_snap_id);
    return;
  }

  if (m_local_snap_id_start > 0 || m_local_snap_id_end != CEPH_NOSNAP) {
    if (m_local_mirror_snap_ns.is_non_primary() &&
        m_local_mirror_snap_ns.primary_mirror_uuid !=
          m_state_builder->remote_mirror_uuid) {
      // TODO support multiple peers
      derr << "local image linked to unknown peer: "
           << m_local_mirror_snap_ns.primary_mirror_uuid << dendl;
      handle_replay_complete(locker, -EEXIST,
                             "local image linked to unknown peer");
      return;
    } else if (m_local_mirror_snap_ns.state ==
                 cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
      dout(5) << "local image promoted" << dendl;
      handle_replay_complete(locker, 0, "force promoted");
      return;
    }

    dout(10) << "found local mirror snapshot: "
             << "local_snap_id_start=" << m_local_snap_id_start << ", "
             << "local_snap_id_end=" << m_local_snap_id_end << ", "
             << "local_snap_ns=" << m_local_mirror_snap_ns << dendl;
    if (!m_local_mirror_snap_ns.is_primary() &&
        m_local_mirror_snap_ns.complete) {
      // our remote sync should start after this completed snapshot
      m_remote_snap_id_start = m_local_mirror_snap_ns.primary_snap_id;
    }
  }

  // we don't have any mirror snapshots or only completed non-primary
  // mirror snapshots
  scan_remote_mirror_snapshots(locker);
}

template <typename I>
void Replayer<I>::scan_remote_mirror_snapshots(
    std::unique_lock<ceph::mutex>* locker) {
  dout(10) << dendl;

  m_pending_snapshots = 0;

  std::set<uint64_t> unlink_snap_ids;
  bool split_brain = false;
  bool remote_demoted = false;
  auto remote_image_ctx = m_state_builder->remote_image_ctx;
  std::shared_lock image_locker{remote_image_ctx->image_lock};
  for (auto snap_info_it = remote_image_ctx->snap_info.begin();
       snap_info_it != remote_image_ctx->snap_info.end(); ++snap_info_it) {
    const auto& snap_ns = snap_info_it->second.snap_namespace;
    auto mirror_ns = boost::get<
      cls::rbd::MirrorSnapshotNamespace>(&snap_ns);
    if (mirror_ns == nullptr) {
      continue;
    }

    dout(15) << "remote mirror snapshot: id=" << snap_info_it->first << ", "
             << "mirror_ns=" << *mirror_ns << dendl;
    remote_demoted = mirror_ns->is_demoted();
    if (!mirror_ns->is_primary() && !mirror_ns->is_non_primary()) {
      derr << "unknown remote mirror snapshot state" << dendl;
      handle_replay_complete(locker, -EINVAL,
                             "invalid remote mirror snapshot state");
      return;
    } else if (mirror_ns->mirror_peer_uuids.count(m_remote_mirror_peer_uuid) ==
                 0) {
      dout(15) << "skipping remote snapshot due to missing mirror peer"
               << dendl;
      continue;
    }

    auto remote_snap_id = snap_info_it->first;
    if (m_local_snap_id_start > 0 || m_local_snap_id_end != CEPH_NOSNAP) {
      // we have a local mirror snapshot
      if (m_local_mirror_snap_ns.is_non_primary()) {
        // previously validated that it was linked to remote
        ceph_assert(m_local_mirror_snap_ns.primary_mirror_uuid ==
                      m_state_builder->remote_mirror_uuid);

        unlink_snap_ids.insert(remote_snap_id);
        if (m_local_mirror_snap_ns.complete &&
            m_local_mirror_snap_ns.primary_snap_id >= remote_snap_id) {
          // skip past completed remote snapshot
          m_remote_snap_id_start = remote_snap_id;
          m_remote_mirror_snap_ns = *mirror_ns;
          dout(15) << "skipping synced remote snapshot " << remote_snap_id
                   << dendl;
          continue;
        } else if (!m_local_mirror_snap_ns.complete &&
                   m_local_mirror_snap_ns.primary_snap_id > remote_snap_id) {
          // skip until we get to the in-progress remote snapshot
          dout(15) << "skipping synced remote snapshot " << remote_snap_id
                   << " while search for in-progress sync" << dendl;
          m_remote_snap_id_start = remote_snap_id;
          m_remote_mirror_snap_ns = *mirror_ns;
          continue;
        }
      } else if (m_local_mirror_snap_ns.state ==
                   cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED) {
        // find the matching demotion snapshot in remote image
        ceph_assert(m_local_snap_id_start > 0);
        if (mirror_ns->state ==
              cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED &&
            mirror_ns->primary_mirror_uuid == m_local_mirror_uuid &&
            mirror_ns->primary_snap_id == m_local_snap_id_start) {
          dout(10) << "located matching demotion snapshot: "
                   << "remote_snap_id=" << remote_snap_id << ", "
                   << "local_snap_id=" << m_local_snap_id_start << dendl;
          m_remote_snap_id_start = remote_snap_id;
          split_brain = false;
          continue;
        } else if (m_remote_snap_id_start == 0) {
          // still looking for our matching demotion snapshot
          dout(15) << "skipping remote snapshot " << remote_snap_id << " "
                   << "while searching for demotion" << dendl;
          split_brain = true;
          continue;
        }
      } else {
        // should not have been able to reach this
        ceph_assert(false);
      }
    } else if (!mirror_ns->is_primary()) {
      dout(15) << "skipping non-primary remote snapshot" << dendl;
      continue;
    }

    // found candidate snapshot to sync
    ++m_pending_snapshots;
    if (m_remote_snap_id_end != CEPH_NOSNAP) {
      continue;
    }

    // first primary snapshot where were are listed as a peer
    m_remote_snap_id_end = remote_snap_id;
    m_remote_mirror_snap_ns = *mirror_ns;
  }
  image_locker.unlock();

  unlink_snap_ids.erase(m_remote_snap_id_start);
  unlink_snap_ids.erase(m_remote_snap_id_end);
  if (!unlink_snap_ids.empty()) {
    locker->unlock();

    // retry the unlinking process for a remote snapshot that we do not
    // need anymore
    auto remote_snap_id = *unlink_snap_ids.begin();
    dout(10) << "unlinking from remote snapshot " << remote_snap_id << dendl;
    unlink_peer(remote_snap_id);
    return;
  }

  if (m_remote_snap_id_end != CEPH_NOSNAP) {
    dout(10) << "found remote mirror snapshot: "
             << "remote_snap_id_start=" << m_remote_snap_id_start << ", "
             << "remote_snap_id_end=" << m_remote_snap_id_end << ", "
             << "remote_snap_ns=" << m_remote_mirror_snap_ns << dendl;
    if (m_remote_mirror_snap_ns.complete) {
      locker->unlock();

      if (m_local_snap_id_end != CEPH_NOSNAP &&
          !m_local_mirror_snap_ns.complete) {
        // attempt to resume image-sync
        dout(10) << "local image contains in-progress mirror snapshot"
                 << dendl;
        get_local_image_state();
      } else {
        copy_snapshots();
      }
      return;
    } else {
      // might have raced with the creation of a remote mirror snapshot
      // so we will need to refresh and rescan once it completes
      dout(15) << "remote mirror snapshot not complete" << dendl;
    }
  }

  if (m_image_updated) {
    // received update notification while scanning image, restart ...
    m_image_updated = false;
    locker->unlock();

    dout(10) << "restarting snapshot scan due to remote update notification"
             << dendl;
    load_local_image_meta();
    return;
  }

  if (is_replay_interrupted(locker)) {
    return;
  } else if (split_brain) {
    derr << "split-brain detected: failed to find matching non-primary "
         << "snapshot in remote image: "
         << "local_snap_id_start=" << m_local_snap_id_start << ", "
         << "local_snap_ns=" << m_local_mirror_snap_ns << dendl;
    handle_replay_complete(locker, -EEXIST, "split-brain");
    return;
  } else if (remote_demoted) {
    dout(10) << "remote image demoted" << dendl;
    handle_replay_complete(locker, -EREMOTEIO, "remote image demoted");
    return;
  }

  dout(10) << "all remote snapshots synced: idling waiting for new snapshot"
           << dendl;
  ceph_assert(m_state == STATE_REPLAYING);
  m_state = STATE_IDLE;

  notify_status_updated();
}

template <typename I>
void Replayer<I>::prune_non_primary_snapshot(uint64_t snap_id) {
  dout(10) << "snap_id=" << snap_id << dendl;

  auto local_image_ctx = m_state_builder->local_image_ctx;
  bool snap_valid = false;
  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;

  {
    std::shared_lock image_locker{local_image_ctx->image_lock};
    auto snap_info = local_image_ctx->get_snap_info(snap_id);
    if (snap_info != nullptr) {
      snap_valid = true;
      snap_namespace = snap_info->snap_namespace;
      snap_name = snap_info->name;

      ceph_assert(boost::get<cls::rbd::MirrorSnapshotNamespace>(
        &snap_namespace) != nullptr);
    }
  }

  if (!snap_valid) {
    load_local_image_meta();
    return;
  }

  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_prune_non_primary_snapshot>(this);
  local_image_ctx->operations->snap_remove(snap_namespace, snap_name, ctx);
}

template <typename I>
void Replayer<I>::handle_prune_non_primary_snapshot(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "failed to prune non-primary snapshot: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(r, "failed to prune non-primary snapshot");
    return;
  }

  if (is_replay_interrupted()) {
    return;
  }

  load_local_image_meta();
}

template <typename I>
void Replayer<I>::copy_snapshots() {
  dout(10) << "remote_snap_id_start=" << m_remote_snap_id_start << ", "
           << "remote_snap_id_end=" << m_remote_snap_id_end << ", "
           << "local_snap_id_start=" << m_local_snap_id_start << dendl;

  ceph_assert(m_remote_snap_id_start != CEPH_NOSNAP);
  ceph_assert(m_remote_snap_id_end > 0 &&
              m_remote_snap_id_end != CEPH_NOSNAP);
  ceph_assert(m_local_snap_id_start != CEPH_NOSNAP);

  m_local_mirror_snap_ns = {};
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_copy_snapshots>(this);
  auto req = librbd::deep_copy::SnapshotCopyRequest<I>::create(
    m_state_builder->remote_image_ctx, m_state_builder->local_image_ctx,
    m_remote_snap_id_start, m_remote_snap_id_end, m_local_snap_id_start,
    false, m_threads->work_queue, &m_local_mirror_snap_ns.snap_seqs,
    ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_copy_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to copy snapshots from remote to local image: "
         << cpp_strerror(r) << dendl;
    handle_replay_complete(
      r, "failed to copy snapshots from remote to local image");
    return;
  }

  dout(10) << "remote_snap_id_start=" << m_remote_snap_id_start << ", "
           << "remote_snap_id_end=" << m_remote_snap_id_end << ", "
           << "local_snap_id_start=" << m_local_snap_id_start << ", "
           << "snap_seqs=" << m_local_mirror_snap_ns.snap_seqs << dendl;
  get_remote_image_state();
}

template <typename I>
void Replayer<I>::get_remote_image_state() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_get_remote_image_state>(this);
  auto req = librbd::mirror::snapshot::GetImageStateRequest<I>::create(
    m_state_builder->remote_image_ctx, m_remote_snap_id_end,
    &m_image_state, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_get_remote_image_state(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to retrieve remote snapshot image state: "
         << cpp_strerror(r) << dendl;
    handle_replay_complete(r, "failed to retrieve remote snapshot image state");
    return;
  }

  create_non_primary_snapshot();
}

template <typename I>
void Replayer<I>::get_local_image_state() {
  dout(10) << dendl;

  ceph_assert(m_local_snap_id_end != CEPH_NOSNAP);
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_get_local_image_state>(this);
  auto req = librbd::mirror::snapshot::GetImageStateRequest<I>::create(
    m_state_builder->local_image_ctx, m_local_snap_id_end,
    &m_image_state, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_get_local_image_state(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to retrieve local snapshot image state: "
         << cpp_strerror(r) << dendl;
    handle_replay_complete(r, "failed to retrieve local snapshot image state");
    return;
  }

  request_sync();
}

template <typename I>
void Replayer<I>::create_non_primary_snapshot() {
  auto local_image_ctx = m_state_builder->local_image_ctx;

  if (m_local_snap_id_start > 0) {
    std::shared_lock local_image_locker{local_image_ctx->image_lock};

    auto local_snap_info_it = local_image_ctx->snap_info.find(
      m_local_snap_id_start);
    if (local_snap_info_it == local_image_ctx->snap_info.end()) {
      local_image_locker.unlock();

      derr << "failed to locate local snapshot " << m_local_snap_id_start
           << dendl;
      handle_replay_complete(-ENOENT, "failed to locate local start snapshot");
      return;
    }

    auto mirror_ns = boost::get<cls::rbd::MirrorSnapshotNamespace>(
      &local_snap_info_it->second.snap_namespace);
    ceph_assert(mirror_ns != nullptr);

    auto remote_image_ctx = m_state_builder->remote_image_ctx;
    std::shared_lock remote_image_locker{remote_image_ctx->image_lock};

    // (re)build a full mapping from remote to local snap ids for all user
    // snapshots to support applying image state in the future
    for (auto& [remote_snap_id, remote_snap_info] :
           remote_image_ctx->snap_info) {
      if (remote_snap_id >= m_remote_snap_id_end) {
        break;
      }

      // we can ignore all non-user snapshots since image state only includes
      // user snapshots
      if (boost::get<cls::rbd::UserSnapshotNamespace>(
            &remote_snap_info.snap_namespace) == nullptr) {
        continue;
      }

      uint64_t local_snap_id = CEPH_NOSNAP;
      if (mirror_ns->is_demoted() && !m_remote_mirror_snap_ns.is_demoted()) {
        // if we are creating a non-primary snapshot following a demotion,
        // re-build the full snapshot sequence since we don't have a valid
        // snapshot mapping
        auto local_snap_id_it = local_image_ctx->snap_ids.find(
          {remote_snap_info.snap_namespace, remote_snap_info.name});
        if (local_snap_id_it != local_image_ctx->snap_ids.end()) {
          local_snap_id = local_snap_id_it->second;
        }
      } else {
        auto snap_seq_it = mirror_ns->snap_seqs.find(remote_snap_id);
        if (snap_seq_it != mirror_ns->snap_seqs.end()) {
          local_snap_id = snap_seq_it->second;
        }
      }

      if (m_local_mirror_snap_ns.snap_seqs.count(remote_snap_id) == 0 &&
          local_snap_id != CEPH_NOSNAP) {
        dout(15) << "mapping remote snapshot " << remote_snap_id << " to "
                 << "local snapshot " << local_snap_id << dendl;
        m_local_mirror_snap_ns.snap_seqs[remote_snap_id] = local_snap_id;
      }
    }
  }

  dout(10) << "demoted=" << m_remote_mirror_snap_ns.is_demoted() << ", "
           << "primary_mirror_uuid="
           << m_state_builder->remote_mirror_uuid << ", "
           << "primary_snap_id=" << m_remote_snap_id_end << ", "
           << "snap_seqs=" << m_local_mirror_snap_ns.snap_seqs << dendl;

  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_create_non_primary_snapshot>(this);
  auto req = librbd::mirror::snapshot::CreateNonPrimaryRequest<I>::create(
    local_image_ctx, m_remote_mirror_snap_ns.is_demoted(),
    m_state_builder->remote_mirror_uuid, m_remote_snap_id_end,
    m_local_mirror_snap_ns.snap_seqs, m_image_state, &m_local_snap_id_end, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_create_non_primary_snapshot(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to create local mirror snapshot: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(r, "failed to create local mirror snapshot");
    return;
  }

  dout(15) << "local_snap_id_end=" << m_local_snap_id_end << dendl;

  request_sync();
}

template <typename I>
void Replayer<I>::request_sync() {
  if (m_remote_mirror_snap_ns.clean_since_snap_id == m_remote_snap_id_start) {
    dout(10) << "skipping unnecessary image copy: "
             << "remote_snap_id_start=" << m_remote_snap_id_start << ", "
             << "remote_mirror_snap_ns=" << m_remote_mirror_snap_ns << dendl;
    apply_image_state();
    return;
  }

  dout(10) << dendl;
  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker)) {
    return;
  }

  auto ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
      Replayer<I>, &Replayer<I>::handle_request_sync>(this));
  m_instance_watcher->notify_sync_request(m_state_builder->local_image_ctx->id,
                                          ctx);
}

template <typename I>
void Replayer<I>::handle_request_sync(int r) {
  dout(10) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  if (is_replay_interrupted(&locker)) {
    return;
  } else if (r == -ECANCELED) {
    dout(5) << "image-sync canceled" << dendl;
    handle_replay_complete(&locker, r, "image-sync canceled");
    return;
  } else if (r < 0) {
    derr << "failed to request image-sync: " << cpp_strerror(r) << dendl;
    handle_replay_complete(&locker, r, "failed to request image-sync");
    return;
  }

  m_sync_in_progress = true;
  locker.unlock();

  copy_image();
}

template <typename I>
void Replayer<I>::copy_image() {
  dout(10) << "remote_snap_id_start=" << m_remote_snap_id_start << ", "
           << "remote_snap_id_end=" << m_remote_snap_id_end << ", "
           << "local_snap_id_start=" << m_local_snap_id_start << ", "
           << "last_copied_object_number="
           << m_local_mirror_snap_ns.last_copied_object_number << ", "
           << "snap_seqs=" << m_local_mirror_snap_ns.snap_seqs << dendl;

  m_snapshot_bytes = 0;
  m_deep_copy_handler = new DeepCopyHandler(this);
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_copy_image>(this);
  auto req = librbd::deep_copy::ImageCopyRequest<I>::create(
    m_state_builder->remote_image_ctx,  m_state_builder->local_image_ctx,
    m_remote_snap_id_start, m_remote_snap_id_end, m_local_snap_id_start, false,
    (m_local_mirror_snap_ns.last_copied_object_number > 0 ?
      librbd::deep_copy::ObjectNumber{
        m_local_mirror_snap_ns.last_copied_object_number} :
      librbd::deep_copy::ObjectNumber{}),
    m_local_mirror_snap_ns.snap_seqs, m_deep_copy_handler, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_copy_image(int r) {
  dout(10) << "r=" << r << dendl;

  delete m_deep_copy_handler;
  m_deep_copy_handler = nullptr;

  if (r < 0) {
    derr << "failed to copy remote image to local image: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(r, "failed to copy remote image");
    return;
  }

  {
    std::unique_lock locker{m_lock};
    m_bytes_per_snapshot(m_snapshot_bytes);
    m_snapshot_bytes = 0;
  }

  apply_image_state();
}

template <typename I>
void Replayer<I>::handle_copy_image_progress(uint64_t object_number,
                                             uint64_t object_count) {
  dout(10) << "object_number=" << object_number << ", "
           << "object_count=" << object_count << dendl;

  std::unique_lock locker{m_lock};
  m_local_mirror_snap_ns.last_copied_object_number = std::min(
    object_number, object_count);
  m_local_object_count = object_count;

  update_non_primary_snapshot(false);
}

template <typename I>
void Replayer<I>::handle_copy_image_read(uint64_t bytes_read) {
  dout(20) << "bytes_read=" << bytes_read << dendl;

  std::unique_lock locker{m_lock};
  m_bytes_per_second(bytes_read);
  m_snapshot_bytes += bytes_read;
}

template <typename I>
void Replayer<I>::apply_image_state() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_apply_image_state>(this);
  auto req = ApplyImageStateRequest<I>::create(
    m_local_mirror_uuid,
    m_state_builder->remote_mirror_uuid,
    m_state_builder->local_image_ctx,
    m_state_builder->remote_image_ctx,
    m_image_state, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_apply_image_state(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "failed to apply remote image state to local image: "
         << cpp_strerror(r) << dendl;
    handle_replay_complete(r, "failed to apply remote image state");
    return;
  }

  std::unique_lock locker{m_lock};
  update_non_primary_snapshot(true);
}

template <typename I>
void Replayer<I>::update_non_primary_snapshot(bool complete) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  if (!complete) {
    // disallow two in-flight updates if this isn't the completion of the sync
    if (m_updating_sync_point) {
      return;
    }
    m_updating_sync_point = true;
  } else {
    m_local_mirror_snap_ns.complete = true;
  }

  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_snapshot_set_copy_progress(
    &op, m_local_snap_id_end, m_local_mirror_snap_ns.complete,
    m_local_mirror_snap_ns.last_copied_object_number);

  auto ctx = new C_TrackedOp(
    m_in_flight_op_tracker, new LambdaContext([this, complete](int r) {
      handle_update_non_primary_snapshot(complete, r);
    }));
  auto aio_comp = create_rados_callback(ctx);
  int r = m_state_builder->local_image_ctx->md_ctx.aio_operate(
    m_state_builder->local_image_ctx->header_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void Replayer<I>::handle_update_non_primary_snapshot(bool complete, int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to update local snapshot progress: " << cpp_strerror(r)
         << dendl;
    if (complete) {
      // only fail if this was the final update
      handle_replay_complete(r, "failed to update local snapshot progress");
      return;
    }
  }

  if (!complete) {
    // periodic sync-point update -- do not advance state machine
    std::unique_lock locker{m_lock};

    ceph_assert(m_updating_sync_point);
    m_updating_sync_point = false;
    return;
  }

  notify_image_update();
}

template <typename I>
void Replayer<I>::notify_image_update() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_notify_image_update>(this);
  m_state_builder->local_image_ctx->notify_update(ctx);
}

template <typename I>
void Replayer<I>::handle_notify_image_update(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to notify local image update: " << cpp_strerror(r) << dendl;
  }

  unlink_peer(m_remote_snap_id_start);
}

template <typename I>
void Replayer<I>::unlink_peer(uint64_t remote_snap_id) {
  if (remote_snap_id == 0) {
    finish_sync();
    return;
  }

  // local snapshot fully synced -- we no longer depend on the sync
  // start snapshot in the remote image
  dout(10) << "remote_snap_id=" << remote_snap_id << dendl;

  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_unlink_peer>(this);
  auto req = librbd::mirror::snapshot::UnlinkPeerRequest<I>::create(
    m_state_builder->remote_image_ctx, remote_snap_id,
    m_remote_mirror_peer_uuid, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_unlink_peer(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "failed to unlink local peer from remote image: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(r, "failed to unlink local peer from remote image");
    return;
  }

  finish_sync();
}

template <typename I>
void Replayer<I>::finish_sync() {
  dout(10) << dendl;

  {
    std::unique_lock locker{m_lock};
    notify_status_updated();

    if (m_sync_in_progress) {
      m_sync_in_progress = false;
      m_instance_watcher->notify_sync_complete(
        m_state_builder->local_image_ctx->id);
    }
  }

  if (is_replay_interrupted()) {
    return;
  }

  load_local_image_meta();
}

template <typename I>
void Replayer<I>::register_local_update_watcher() {
  dout(10) << dendl;

  m_update_watch_ctx = new C_UpdateWatchCtx(this);

  int r = m_state_builder->local_image_ctx->state->register_update_watcher(
    m_update_watch_ctx, &m_local_update_watcher_handle);
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_register_local_update_watcher>(this);
  m_threads->work_queue->queue(ctx, r);
}

template <typename I>
void Replayer<I>::handle_register_local_update_watcher(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to register local update watcher: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(r, "failed to register local image update watcher");
    m_state = STATE_COMPLETE;

    delete m_update_watch_ctx;
    m_update_watch_ctx = nullptr;

    Context* on_init = nullptr;
    std::swap(on_init, m_on_init_shutdown);
    on_init->complete(r);
    return;
  }

  register_remote_update_watcher();
}

template <typename I>
void Replayer<I>::register_remote_update_watcher() {
  dout(10) << dendl;

  int r = m_state_builder->remote_image_ctx->state->register_update_watcher(
    m_update_watch_ctx, &m_remote_update_watcher_handle);
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_register_remote_update_watcher>(this);
  m_threads->work_queue->queue(ctx, r);
}

template <typename I>
void Replayer<I>::handle_register_remote_update_watcher(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to register remote update watcher: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(r, "failed to register remote image update watcher");
    m_state = STATE_COMPLETE;

    unregister_local_update_watcher();
    return;
  }

  m_state = STATE_REPLAYING;

  Context* on_init = nullptr;
  std::swap(on_init, m_on_init_shutdown);
  on_init->complete(0);

  // delay initial snapshot scan until after we have alerted
  // image replayer that we have initialized in case an error
  // occurs
  {
    std::unique_lock locker{m_lock};
    notify_status_updated();
  }

  load_local_image_meta();
}

template <typename I>
void Replayer<I>::unregister_remote_update_watcher() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    Replayer<I>,
    &Replayer<I>::handle_unregister_remote_update_watcher>(this);
  m_state_builder->remote_image_ctx->state->unregister_update_watcher(
    m_remote_update_watcher_handle, ctx);
}

template <typename I>
void Replayer<I>::handle_unregister_remote_update_watcher(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to unregister remote update watcher: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(
      r, "failed to unregister remote image update watcher");
  }

  unregister_local_update_watcher();
}

template <typename I>
void Replayer<I>::unregister_local_update_watcher() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    Replayer<I>,
    &Replayer<I>::handle_unregister_local_update_watcher>(this);
  m_state_builder->local_image_ctx->state->unregister_update_watcher(
    m_local_update_watcher_handle, ctx);
}

template <typename I>
void Replayer<I>::handle_unregister_local_update_watcher(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to unregister local update watcher: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(
      r, "failed to unregister local image update watcher");
  }

  delete m_update_watch_ctx;
  m_update_watch_ctx = nullptr;

  wait_for_in_flight_ops();
}

template <typename I>
void Replayer<I>::wait_for_in_flight_ops() {
  dout(10) << dendl;

  auto ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
      Replayer<I>, &Replayer<I>::handle_wait_for_in_flight_ops>(this));
  m_in_flight_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void Replayer<I>::handle_wait_for_in_flight_ops(int r) {
  dout(10) << "r=" << r << dendl;

  Context* on_shutdown = nullptr;
  {
    std::unique_lock locker{m_lock};
    ceph_assert(m_on_init_shutdown != nullptr);
    std::swap(on_shutdown, m_on_init_shutdown);
  }
  on_shutdown->complete(m_error_code);
}

template <typename I>
void Replayer<I>::handle_image_update_notify() {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  if (m_state == STATE_REPLAYING) {
    dout(15) << "flagging snapshot rescan required" << dendl;
    m_image_updated = true;
  } else if (m_state == STATE_IDLE) {
    m_state = STATE_REPLAYING;
    locker.unlock();

    dout(15) << "restarting idle replayer" << dendl;
    load_local_image_meta();
  }
}

template <typename I>
void Replayer<I>::handle_replay_complete(int r,
                                         const std::string& description) {
  std::unique_lock locker{m_lock};
  handle_replay_complete(&locker, r, description);
}

template <typename I>
void Replayer<I>::handle_replay_complete(std::unique_lock<ceph::mutex>* locker,
                                         int r,
                                         const std::string& description) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  if (m_error_code == 0) {
    m_error_code = r;
    m_error_description = description;
  }

  if (m_sync_in_progress) {
    m_sync_in_progress = false;
    m_instance_watcher->notify_sync_complete(
      m_state_builder->local_image_ctx->id);
  }

  if (m_state != STATE_REPLAYING && m_state != STATE_IDLE) {
    return;
  }

  m_state = STATE_COMPLETE;
  notify_status_updated();
}

template <typename I>
void Replayer<I>::notify_status_updated() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  dout(10) << dendl;
  auto ctx = new C_TrackedOp(m_in_flight_op_tracker, new LambdaContext(
    [this](int) {
      m_replayer_listener->handle_notification();
    }));
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
bool Replayer<I>::is_replay_interrupted() {
  std::unique_lock locker{m_lock};
  return is_replay_interrupted(&locker);
}

template <typename I>
bool Replayer<I>::is_replay_interrupted(std::unique_lock<ceph::mutex>* locker) {
  if (m_state == STATE_COMPLETE) {
    locker->unlock();

    dout(10) << "resuming pending shut down" << dendl;
    unregister_remote_update_watcher();
    return true;
  }
  return false;
}

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::snapshot::Replayer<librbd::ImageCtx>;
