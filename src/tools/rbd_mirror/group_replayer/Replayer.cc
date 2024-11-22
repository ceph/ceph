// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Replayer.h"
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

template <typename I>
Replayer<I>::Replayer(
    Threads<I>* threads,
    librados::IoCtx &local_io_ctx,
    librados::IoCtx &remote_io_ctx,
    const std::string &global_group_id,
    const std::string& local_mirror_uuid,
    const std::string& remote_mirror_uuid,
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
    m_remote_mirror_uuid(remote_mirror_uuid),
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
void Replayer<I>::schedule_load_group_snapshots() {
  dout(10) << dendl;

  auto ctx = new LambdaContext(
    [this](int r) {
      load_local_group_snapshots();
    });
  std::lock_guard timer_locker{m_threads->timer_lock};
  m_threads->timer->add_event_after(1, ctx);
}

template <typename I>
void Replayer<I>::notify_group_listener_stop() {
  dout(10) << dendl;

  Context *ctx = new LambdaContext([this](int) {
      m_local_group_ctx->listener->stop();
      });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
int Replayer<I>::local_group_image_list_by_id(
    std::vector<cls::rbd::GroupImageStatus> *image_ids) {
  std::string group_header_oid = librbd::util::group_header_name(
      m_local_group_id);

  dout(10) << "local_group_id=" << m_local_group_id << dendl;
  image_ids->clear();

  int r = 0;
  const int max_read = 1024;
  cls::rbd::GroupImageSpec start_last;
  do {
    std::vector<cls::rbd::GroupImageStatus> image_ids_page;

    r = librbd::cls_client::group_image_list(&m_local_io_ctx, group_header_oid,
                                             start_last, max_read,
                                             &image_ids_page);

    if (r < 0) {
      derr << "error reading image list from local group: "
           << cpp_strerror(-r) << dendl;
      return r;
    }
    image_ids->insert(image_ids->end(), image_ids_page.begin(),
                      image_ids_page.end());

    if (image_ids_page.size() > 0)
      start_last = image_ids_page.rbegin()->spec;

    r = image_ids_page.size();
  } while (r == max_read);

  return 0;
}


template <typename I>
bool Replayer<I>::is_resync_requested() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;

  std::string group_header_oid = librbd::util::group_header_name(
      m_local_group_id);
  std::string value;
  int r = librbd::cls_client::metadata_get(&m_local_io_ctx, group_header_oid,
                                           RBD_GROUP_RESYNC, &value);
  if (r < 0 && r != -ENOENT) {
    derr << "failed reading metadata: " << cpp_strerror(r) << dendl;
  } else if (r == 0) {
    return true;
  }

  return false;
}

template <typename I>
bool Replayer<I>::is_rename_requested() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;

  std::string remote_group_name;
  int r = librbd::cls_client::dir_get_name(&m_remote_io_ctx,
                                           RBD_GROUP_DIRECTORY,
                                           m_remote_group_id,
                                           &remote_group_name);
  if (r < 0) {
    derr << "failed to retrieve remote group name: "
         << cpp_strerror(r) << dendl;
    return false;
  }

  if (m_local_group_ctx && m_local_group_ctx->name != remote_group_name) {
    return true;
  }

  return false;
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

  m_remote_mirror_peer_uuid = remote_pool_meta.mirror_peer_uuid;
  dout(10) << "remote_mirror_peer_uuid=" << m_remote_mirror_peer_uuid << dendl;

  on_finish->complete(0);
  load_local_group_snapshots();
}

template <typename I>
void Replayer<I>::load_local_group_snapshots() {
  dout(10) << "m_local_group_id=" << m_local_group_id << dendl;

  if (m_state != STATE_COMPLETE) {
    m_state = STATE_REPLAYING;
  }

  if (m_stop_requested) {
    return;
  } else if (is_resync_requested()) {
    m_stop_requested = true;
    dout(10) << "local group resync requested" << dendl;
    // send stop for Group Replayer
    notify_group_listener_stop();
  } else if (is_rename_requested()) {
    m_stop_requested = true;
    dout(10) << "remote group rename requested" << dendl;
    // send stop for Group Replayer
    notify_group_listener_stop();
  }

  std::unique_lock locker{m_lock};
  m_local_group_snaps.clear();
  auto ctx = create_context_callback<
      Replayer<I>,
      &Replayer<I>::handle_load_local_group_snapshots>(this);

  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_local_io_ctx,
      m_local_group_id, true, true, &m_local_group_snaps, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_load_local_group_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error listing local mirror group snapshots: " << cpp_strerror(r)
         << dendl;
    schedule_load_group_snapshots();
    return;
  }

  for (auto it = m_local_group_snaps.rbegin();
       it != m_local_group_snaps.rend(); it++) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &it->snapshot_namespace);
    if (ns == nullptr) {
      continue;
    }
    if (ns->state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
      break;
    }
    // this is primary, IDLE the group replayer
    m_state = STATE_IDLE;
    return;
  }

  load_remote_group_snapshots();
}

template <typename I>
void Replayer<I>::load_remote_group_snapshots() {
  dout(10) << "m_remote_group_id=" << m_remote_group_id << dendl;

  std::unique_lock locker{m_lock};
  m_remote_group_snaps.clear();
  auto ctx = new LambdaContext(
    [this] (int r) {
      handle_load_remote_group_snapshots(r);
  });

  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_remote_io_ctx,
      m_remote_group_id, true, true, &m_remote_group_snaps, ctx);
  req->send();
}

template <typename I>
void Replayer<I>::handle_load_remote_group_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error listing remote mirror group snapshots: " << cpp_strerror(r)
         << dendl;
    load_remote_group_snapshots();
    return;
  }

  if (!m_local_group_snaps.empty()) {
    auto last_local_snap = m_local_group_snaps.rbegin();
    if (last_local_snap->state != cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
      // do not forward until previous local group snapshot is COMPLETE
      validate_image_snaps_sync_complete(last_local_snap->id);
      schedule_load_group_snapshots();
      return;
    }

    auto last_local_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &last_local_snap->snapshot_namespace);
    if (last_local_snap_ns &&
        last_local_snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED &&
        !m_remote_group_snaps.empty()) {
      auto last_remote_snap = m_remote_group_snaps.rbegin();
      if (last_local_snap->id == last_remote_snap->id) {
        m_stop_requested = true;
        notify_group_listener_stop();
        schedule_load_group_snapshots();
        return;
      }
    }
  }

  std::unique_lock locker{m_lock};
  scan_for_unsynced_group_snapshots(locker);
}

template <typename I>
void Replayer<I>::validate_image_snaps_sync_complete(
    const std::string &remote_group_snap_id) {
  std::unique_lock locker{m_lock};
  // 1. get group membership
  // 2. get snap list of each image and check any image snap has the group
  // snapid and is set to complete. If yes call complete
  dout(10) << "group snap_id: " << remote_group_snap_id << dendl;

  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  if (itr == m_remote_group_snaps.end()) {
    return;
  }

  // TODO: take care about image addition scenario to group
  std::vector<cls::rbd::GroupImageStatus> local_images;
  int r = local_group_image_list_by_id(&local_images);
  if (r < 0) {
    derr << "failed group image list: " << cpp_strerror(r) << dendl;
    return;
  }

  // Check the image snapshot count in the remote group snap
  if (itr->snaps.size() > local_images.size()) {
    dout(20) << "group membership changed, will retry later, remote snapshot images count: "
         << itr->snaps.size() << ", local group images count: "
         << local_images.size() << dendl;
    // need restart of group replayer to pull the new images,
    // which will start/stop fresh set of Imagereplayers
    m_stop_requested = true;
    notify_group_listener_stop();
    return;
  }

  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });
  if (itl == m_local_group_snaps.end()) {
    return;
  }
  auto snap_type = cls::rbd::get_group_snap_namespace_type(
      itl->snapshot_namespace);
  if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
    C_SaferCond *ctx = new C_SaferCond;
    regular_snapshot_complete(remote_group_snap_id, ctx);
    ctx->wait();
    return;
  }

  // FIXME: get the latest image spec added to a different pool,
  // for now searching only in the local pool.

  // 1. Get remote image_id
  // 2. Translate to local image id
  // 3. get the image spec

  if (itr->snaps.size() == 0) {
    dout(5) << "Image list is empty!!" << dendl;
    C_SaferCond *ctx = new C_SaferCond;
    mirror_snapshot_complete(remote_group_snap_id, nullptr, ctx);
    ctx->wait();
    return;
  }
  for (auto& image : local_images) {
    std::string image_header_oid = librbd::util::header_name(
        image.spec.image_id);
    ::SnapContext snapc;
    int r = librbd::cls_client::get_snapcontext(&m_local_io_ctx,
        image_header_oid, &snapc);
    if (r < 0) {
      derr << "get snap context failed: " << cpp_strerror(r) << dendl;
      return;
    }

    // stored in reverse order
    for (auto snap_id : snapc.snaps) {
      cls::rbd::SnapshotInfo snap_info;
      r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
          snap_id, &snap_info);
      if (r < 0) {
        derr << "failed getting snap info for snap id: " << snap_id
          << ", : " << cpp_strerror(r) << dendl;
        return;
      }
      auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
          &snap_info.snapshot_namespace);
      if (!mirror_ns) {
        continue;
      }
      // Makesure the image snapshot is COMPLETE
      if (mirror_ns->group_snap_id == remote_group_snap_id && mirror_ns->complete) {
        cls::rbd::ImageSnapshotSpec snap_spec;
        snap_spec.pool = image.spec.pool_id;
        snap_spec.image_id = image.spec.image_id;
        snap_spec.snap_id = snap_info.id;
        auto it = std::find_if(
          itl->snaps.begin(), itl->snaps.end(),
          [&snap_spec](const cls::rbd::ImageSnapshotSpec &s) {
            return snap_spec.pool == s.pool && snap_spec.image_id == s.image_id;
          });
        // Send to only if the image spec absent in INCOMPLETE group Snapshot
        if (it == itl->snaps.end()) {
          C_SaferCond *ctx = new C_SaferCond;
          mirror_snapshot_complete(remote_group_snap_id, &snap_spec, ctx);
          ctx->wait();
        }
        continue;
      } else {
        dout(10) << "remote group snap id: " << remote_group_snap_id
                 << ", local reflected in the image snap: "
                 << mirror_ns->group_snap_id << dendl;
      }
    }
  }
}


template <typename I>
void Replayer<I>::scan_for_unsynced_group_snapshots(
    std::unique_lock<ceph::mutex> &locker) {
  dout(10) << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  bool found = false;
  bool syncs_upto_date = false;
  if (m_remote_group_snaps.empty()) {
    goto out;
  }

  // check if we have a matching snap on remote to start with.
  for (auto local_snap = m_local_group_snaps.rbegin();
       local_snap != m_local_group_snaps.rend(); ++local_snap) {
    auto snap_type = cls::rbd::get_group_snap_namespace_type(
        local_snap->snapshot_namespace);
    auto local_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &local_snap->snapshot_namespace);
    auto next_remote_snap = m_remote_group_snaps.end();
    if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER ||
        (local_snap_ns && (local_snap_ns->is_non_primary() ||
        local_snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED))) {
      for (auto remote_snap = m_remote_group_snaps.begin();
           remote_snap != m_remote_group_snaps.end(); ++remote_snap) {
        if (local_snap->id == remote_snap->id) {
          next_remote_snap = std::next(remote_snap);
          found = true;
          break;
        }
      }
    }
    if (found && next_remote_snap == m_remote_group_snaps.end()) {
      syncs_upto_date = true;
      break;
    }
    if (next_remote_snap != m_remote_group_snaps.end()) {
      auto id = next_remote_snap->id;
      auto itl = std::find_if(
          m_local_group_snaps.begin(), m_local_group_snaps.end(),
          [id](const cls::rbd::GroupSnapshot &s) {
          return s.id == id;
          });
      if (found && itl == m_local_group_snaps.end()) {
        try_create_group_snapshot(*next_remote_snap);
        locker.unlock();
        return;
      }
    }
    found = false;
  }
  if (!syncs_upto_date) {
    dout(10) << "none of the local snaps match remote" << dendl;
    auto remote_snap = m_remote_group_snaps.rbegin();
    for(; remote_snap != m_remote_group_snaps.rend(); ++remote_snap) {
      auto prev_remote_snap = std::next(remote_snap);
      if (prev_remote_snap == m_remote_group_snaps.rend()) {
        break;
      }
      auto snap_type = cls::rbd::get_group_snap_namespace_type(
          prev_remote_snap->snapshot_namespace);
      if (snap_type != cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
        continue;
      }
      auto prev_remote_snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
          &prev_remote_snap->snapshot_namespace);
      if (prev_remote_snap_ns && prev_remote_snap_ns->is_demoted()) {
        break;
      }
    }
    auto id = remote_snap->id;
    auto itl = std::find_if(
        m_local_group_snaps.begin(), m_local_group_snaps.end(),
        [id](const cls::rbd::GroupSnapshot &s) {
        return s.id == id;
        });
    if (remote_snap != m_remote_group_snaps.rend() &&
        itl == m_local_group_snaps.end()) {
      try_create_group_snapshot(*remote_snap);
      locker.unlock();
      return;
    }
  }

out:
  // At this point all group snapshots have been synced, but we keep poll
  locker.unlock();
  if (m_stop_requested) {
    // stop group replayer
    notify_group_listener_stop();
  }
  schedule_load_group_snapshots();
}

template <typename I>
std::string Replayer<I>::prepare_non_primary_mirror_snap_name(
    const std::string &global_group_id,
    const std::string &snap_id) {
  dout(5) << "global_group_id: " << global_group_id
          << ", snap_id: " << snap_id << dendl;
  std::stringstream ind_snap_name_stream;
  ind_snap_name_stream << ".mirror.non-primary."
                       << global_group_id << "." << snap_id;
  return ind_snap_name_stream.str();
}

template <typename I>
void Replayer<I>::try_create_group_snapshot(cls::rbd::GroupSnapshot snap) {
  dout(10) << snap.id << dendl;

  auto snap_type = cls::rbd::get_group_snap_namespace_type(
      snap.snapshot_namespace);
  if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
    auto snap_ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &snap.snapshot_namespace);
    if (snap_ns->is_non_primary()) {
      dout(10) << "remote group snapshot: " << snap.id << "is non primary"
               << dendl;
      return;
    }
    auto snap_state =
      snap_ns->state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY ?
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY :
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED;
    create_mirror_snapshot(snap.id, snap_state);
  } else if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
    dout(10) << "found regular snap, snap name: " << snap.name
             << ", remote group snap id: " << snap.id << dendl;
    C_SaferCond *ctx = new C_SaferCond;
    create_regular_snapshot(snap.name, snap.id, ctx);
    ctx->wait();
  }
}

template <typename I>
void Replayer<I>::create_mirror_snapshot(
    const std::string &remote_group_snap_id,
    const cls::rbd::MirrorSnapshotState &snap_state) {
  dout(10) << remote_group_snap_id << dendl;

  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  if (itl != m_local_group_snaps.end() &&
      itl->state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
    dout(20) << "group snapshot: " << remote_group_snap_id << " already exists"
             << dendl;
    schedule_load_group_snapshots();
    return;
  }

  auto requests_it = m_create_snap_requests.find(remote_group_snap_id);
  if (requests_it == m_create_snap_requests.end()) {
    requests_it = m_create_snap_requests.insert(
        {remote_group_snap_id, {}}).first;
    cls::rbd::GroupSnapshot local_snap =
      {remote_group_snap_id,
       cls::rbd::GroupSnapshotNamespaceMirror{
         snap_state, {}, m_remote_mirror_uuid, remote_group_snap_id},
       {}, cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
    local_snap.name = prepare_non_primary_mirror_snap_name(m_global_group_id,
        remote_group_snap_id);
    m_local_group_snaps.push_back(local_snap);

    auto comp = create_rados_callback(
      new LambdaContext([this, remote_group_snap_id](int r) {
        handle_create_mirror_snapshot(remote_group_snap_id, r);
      }));

    librados::ObjectWriteOperation op;
    librbd::cls_client::group_snap_set(&op, local_snap);
    int r = m_local_io_ctx.aio_operate(
        librbd::util::group_header_name(m_local_group_id), comp, &op);
    ceph_assert(r == 0);
    comp->release();
  } else {
   schedule_load_group_snapshots();
  }
}

template <typename I>
void Replayer<I>::handle_create_mirror_snapshot(
    const std::string &remote_group_snap_id, int r) {
  dout(10) << remote_group_snap_id << ", r=" << r << dendl;

  schedule_load_group_snapshots();
}

template <typename I>
void Replayer<I>::mirror_snapshot_complete(
    const std::string &remote_group_snap_id,
    cls::rbd::ImageSnapshotSpec *spec,
    Context *on_finish) {

  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  ceph_assert(itr != m_remote_group_snaps.end());
  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });
  if (itr->snaps.size() != 0) {
    // update the group snap with snap spec
    itl->snaps.push_back(*spec);
  }

  if (itr->snaps.size() == itl->snaps.size()) {
    m_create_snap_requests.erase(remote_group_snap_id);
    itl->state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
  }

  dout(10) << "local group snap info: "
           << "id: " << itl->id
           << ", name: " << itl->name
           << ", state: " << itl->state
           << ", snaps.size: " << itl->snaps.size()
           << dendl;
  auto comp = create_rados_callback(
    new LambdaContext([this, remote_group_snap_id, on_finish](int r) {
      handle_mirror_snapshot_complete(r, remote_group_snap_id, on_finish);
    }));

  librados::ObjectWriteOperation op;
  librbd::cls_client::group_snap_set(&op, *itl);
  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void Replayer<I>::handle_mirror_snapshot_complete(
    int r, const std::string &remote_group_snap_id, Context *on_finish) {
  dout(10) << remote_group_snap_id << ", r=" << r << dendl;

  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  if (itl->state !=
      cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
    on_finish->complete(0);
    return;
  }

  // remove mirror_peer_uuids from remote snap
  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  ceph_assert(itr != m_remote_group_snaps.end());
  auto rns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &itr->snapshot_namespace);
  if (rns != nullptr) {
    rns->mirror_peer_uuids.clear();
    auto comp = create_rados_callback(
        new LambdaContext([this, remote_group_snap_id](int r) {
          unlink_group_snapshots(remote_group_snap_id);
        }));

    librados::ObjectWriteOperation op;
    librbd::cls_client::group_snap_set(&op, *itr);
    int r = m_remote_io_ctx.aio_operate(
        librbd::util::group_header_name(m_remote_group_id), comp, &op);
    ceph_assert(r == 0);
    comp->release();
  }

  on_finish->complete(0);
}

template <typename I>
void Replayer<I>::unlink_group_snapshots(
    const std::string &remote_group_snap_id) {
  if (m_image_replayers->empty()) {
    return;
  }
  dout(10) << dendl;
  int r;
  for (auto &snap : m_local_group_snaps) {
    if (snap.id == remote_group_snap_id) {
      break;
    }
    auto snap_type = cls::rbd::get_group_snap_namespace_type(
        snap.snapshot_namespace);
    if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
      bool unlink_user_snap = true;
      for (auto &remote_snap : m_remote_group_snaps) {
        if (remote_snap.name == snap.name) {
          unlink_user_snap = false;
          break;
        }
      }
      if (!unlink_user_snap) {
        continue;
      }
      dout(10) << "unlinking regular group snap in-progress: "
               << snap.name << ", with id: " << snap.id << dendl;
    }
    dout(10) << "attempting to unlink image snaps from group snap: "
             << snap.id << dendl;
    bool retain = false;
    for (auto &spec : snap.snaps) {
      std::string image_header_oid = librbd::util::header_name(spec.image_id);
      cls::rbd::SnapshotInfo snap_info;
      r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
          spec.snap_id, &snap_info);
      if (r == -ENOENT) {
        continue;
      } else if (r < 0) {
        derr << "failed getting snap info for snap id: " << spec.snap_id
             << ", : " << cpp_strerror(r) << dendl;
      }
      retain = true;
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
        image_replayer->prune_snapshot(spec.snap_id);
        break;
      }
    }
    // ImageReplayer must be down, do it later.
    if (retain) {
      continue;
    }
    dout(10) << "all image snaps are pruned, finally unlinking group snap: "
             << snap.id << dendl;
    r = librbd::cls_client::group_snap_remove(&m_local_io_ctx,
        librbd::util::group_header_name(m_local_group_id), snap.id);
    if (r < 0) {
      derr << "failed to remove group snapshot : "
           << snap.id << " : " << cpp_strerror(r) << dendl;
    }
  }
}

template <typename I>
void Replayer<I>::create_regular_snapshot(
    const std::string &remote_group_snap_name,
    const std::string &remote_group_snap_id,
    Context *on_finish) {
  dout(10) << remote_group_snap_id << dendl;
  librados::ObjectWriteOperation op;
  cls::rbd::GroupSnapshot group_snap{
    remote_group_snap_id, // keeping it same as remote group snap id
    cls::rbd::GroupSnapshotNamespaceUser{},
      remote_group_snap_name,
      cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};

  librbd::cls_client::group_snap_set(&op, group_snap);
  auto comp = create_rados_callback(
      new LambdaContext([this, on_finish](int r) {
        handle_create_regular_snapshot(r, on_finish);
      }));
  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void Replayer<I>::handle_create_regular_snapshot(
    int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error creating local non-primary group snapshot: "
         << cpp_strerror(r) << dendl;
  }
  on_finish->complete(0);

  schedule_load_group_snapshots();
}

template <typename I>
void Replayer<I>::regular_snapshot_complete(
    const std::string &remote_group_snap_id,
    Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  auto itl = std::find_if(
      m_local_group_snaps.begin(), m_local_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });
  if (itl == m_local_group_snaps.end()) {
    on_finish->complete(0);
    return;
  }

  auto itr = std::find_if(
      m_remote_group_snaps.begin(), m_remote_group_snaps.end(),
      [remote_group_snap_id](const cls::rbd::GroupSnapshot &s) {
      return s.id == remote_group_snap_id;
      });

  // each image will have one snapshot specific to group snap, and so for each
  // image get a ImageSnapshotSpec and prepare a vector
  // for image :: <images in that group> {
  //   * get snap whos name has group snap_id for that we can list snaps and
  //     filter with remote_group_snap_id
  //   * get its { pool_id, snap_id, image_id }
  // }
  // finally write to the object

  std::vector<cls::rbd::ImageSnapshotSpec> local_image_snap_specs;
  if (itr != m_remote_group_snaps.end()) {
    local_image_snap_specs.reserve(itr->snaps.size());
    std::vector<cls::rbd::GroupImageStatus> local_images;
    int r = local_group_image_list_by_id(&local_images);
    if (r < 0) {
      derr << "failed group image list: " << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }
    for (auto& image : local_images) {
      std::string image_header_oid = librbd::util::header_name(
          image.spec.image_id);
      ::SnapContext snapc;
      int r = librbd::cls_client::get_snapcontext(&m_local_io_ctx,
          image_header_oid, &snapc);
      if (r < 0) {
        derr << "get snap context failed: " << cpp_strerror(r) << dendl;
        on_finish->complete(r);
        return;
      }

      auto image_snap_name = ".group." + std::to_string(image.spec.pool_id) +
        "_" + m_remote_group_id + "_" + remote_group_snap_id;
      // stored in reverse order
      for (auto snap_id : snapc.snaps) {
        cls::rbd::SnapshotInfo snap_info;
        r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
            snap_id, &snap_info);
        if (r < 0) {
          derr << "failed getting snap info for snap id: " << snap_id
            << ", : " << cpp_strerror(r) << dendl;
          on_finish->complete(r);
          return;
        }

        // extract { pool_id, snap_id, image_id }
        if (snap_info.name == image_snap_name) {
          cls::rbd::ImageSnapshotSpec snap_spec;
          snap_spec.pool = image.spec.pool_id;
          snap_spec.image_id = image.spec.image_id;
          snap_spec.snap_id = snap_info.id;

          local_image_snap_specs.push_back(snap_spec);
        }
      }
    }
  }

  if (itr->snaps.size() == local_image_snap_specs.size()) {
    itl->snaps = local_image_snap_specs;
    itl->state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
  }
  librados::ObjectWriteOperation op;
  librbd::cls_client::group_snap_set(&op, *itl);

  auto comp = create_rados_callback(
      new LambdaContext([this, on_finish](int r) {
        handle_regular_snapshot_complete(r, on_finish);
      }));
  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void Replayer<I>::handle_regular_snapshot_complete(
    int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;
  on_finish->complete(0);
}

template <typename I>
void Replayer<I>::shut_down(Context* on_finish) {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  m_stop_requested = true;
  auto state = STATE_COMPLETE;
  std::swap(m_state, state);
  locker.unlock();
  if (on_finish) {
    on_finish->complete(0);
  }
  return;
}


} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::Replayer<librbd::ImageCtx>;
