// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "BootstrapRequest.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/internal.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/group/RemoveImageRequest.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_deleter/TrashMoveRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::" \
                           << "BootstrapRequest: " << " " \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

namespace {

static const uint32_t MAX_RETURN = 1024;

bool is_demoted_snap_exists(
    const std::vector<cls::rbd::GroupSnapshot> &snaps) {
  for (auto it = snaps.rbegin(); it != snaps.rend(); it++) {
     auto ns = std::get_if<cls::rbd::MirrorGroupSnapshotNamespace>(
       &it->snapshot_namespace);
    if (ns != nullptr) {
      if (ns->is_demoted()) {
        return true;
      }
    }
  }
  return false;
}

int get_last_mirror_snapshot_state(
    const std::vector<cls::rbd::GroupSnapshot> &snaps,
    cls::rbd::MirrorSnapshotState *state) {
  for (auto it = snaps.rbegin(); it != snaps.rend(); it++) {
    auto ns = std::get_if<cls::rbd::MirrorGroupSnapshotNamespace>(
        &it->snapshot_namespace);
    if (ns != nullptr) {
      // XXXMG: check primary_mirror_uuid matches?
      *state = ns->state;
      return 0;
    }
  }

  return -ENOENT;
}

} // anonymous namespace

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
    bool resync_requested,
    std::string *local_group_id,
    std::string *remote_group_id,
    std::map<std::string, cls::rbd::GroupSnapshot> *local_group_snaps,
    GroupCtx *local_group_ctx,
    std::list<std::pair<librados::IoCtx, ImageReplayer<I> *>> *image_replayers,
    std::map<std::pair<int64_t, std::string>, ImageReplayer<I> *> *image_replayer_index,
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
    m_local_group_id(local_group_id),
    m_remote_group_id(remote_group_id),
    m_local_group_snaps(local_group_snaps),
    m_local_group_ctx(local_group_ctx),
    m_image_replayers(image_replayers),
    m_image_replayer_index(image_replayer_index),
    m_on_finish(on_finish) {
  dout(10)  << "global_group_id=" << m_global_group_id << dendl;
}

template <typename I>
void BootstrapRequest<I>::send() {
  if (m_resync_requested) {
    get_local_group_id();
  } else {
    get_remote_group_id();
  }
}

template <typename I>
void BootstrapRequest<I>::cancel() {
  dout(10) << dendl;

  m_canceled = true;
}

template <typename I>
std::string BootstrapRequest<I>::prepare_non_primary_mirror_snap_name(
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
bool BootstrapRequest<I>::has_remote_image(
  int64_t local_pool_id, const std::string &global_image_id) const {

  std::string pool_name;
  int r = librados::Rados(m_local_io_ctx).pool_reverse_lookup(local_pool_id,
                                                              &pool_name);
  if (r < 0) {
    return false;
  }
  int64_t remote_pool_id =
      librados::Rados(m_remote_io_ctx).pool_lookup(pool_name.c_str());
  if (remote_pool_id < 0) {
    return false;
  }

  return m_remote_images.count({remote_pool_id, global_image_id}) > 0;
}

template <typename I>
void BootstrapRequest<I>::get_remote_group_id() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_group_get_group_id_start(&op, m_global_group_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_get_remote_group_id>(this);

  int r = m_remote_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_group_id(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  if (r == -ENOENT) {
    get_local_group_id();
    return;
  }

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_group_get_group_id_finish(
        &iter, m_remote_group_id);
  }

  if (r < 0) {
    derr << "error getting remote group id: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_remote_group_name();
}

template <typename I>
void BootstrapRequest<I>::get_remote_group_name() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, *m_remote_group_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_get_remote_group_name>(this);

  int r = m_remote_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op,
                                      &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_group_name(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  if (r == -ENOENT) {
    get_local_group_id();
    return;
  }

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_name_finish(&iter, &m_group_name);
  }

  if (r < 0) {
    derr << "error getting remote group name: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_remote_mirror_group();
}

template <typename I>
void BootstrapRequest<I>::get_remote_mirror_group() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_group_get_start(&op, *m_remote_group_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_get_remote_mirror_group>(this);

  int r = m_remote_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  if (r == -ENOENT) {
    m_remote_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLED;
    get_local_group_id();
    return;
  }

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_group_get_finish(&iter,
                                                    &m_remote_mirror_group);
  }

  if (r < 0 && r != -ENOENT) {
    derr << "error getting remote mirror group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_remote_mirror_group.global_group_id != m_global_group_id) {
    derr << "invalid global group id: "
         << m_remote_mirror_group.global_group_id << dendl;
    finish(-EINVAL);
    return;
  }

  list_remote_group_snapshots();
}

template <typename I>
void BootstrapRequest<I>::list_remote_group_snapshots() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_list_remote_group_snapshots>(this);

  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_remote_io_ctx,
      *m_remote_group_id, true, true, &remote_group_snaps, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_list_remote_group_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  if (r < 0) {
    derr << "error listing remote mirror group snapshots: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  if (m_remote_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
    cls::rbd::MirrorSnapshotState state;
    r = get_last_mirror_snapshot_state(remote_group_snaps, &state);
    if (r == -ENOENT) {
      derr << "failed to find remote mirror group snapshot" << dendl;
      finish(-EINVAL);
      return;
    }
    ceph_assert(r == 0);
    m_remote_mirror_group_primary = (state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY);
  }

  if (m_remote_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
      m_remote_mirror_group_primary) {
    list_remote_group();
  } else {
    get_local_group_id();
  }
}

template <typename I>
void BootstrapRequest<I>::list_remote_group() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  cls::rbd::GroupImageSpec start_after;
  if (!m_images.empty()) {
    start_after = m_images.rbegin()->spec;
  }
  librbd::cls_client::group_image_list_start(&op, start_after, MAX_RETURN);

  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_list_remote_group>(this);
  m_out_bl.clear();
  int r = m_remote_io_ctx.aio_operate(
      librbd::util::group_header_name(*m_remote_group_id), comp, &op,
      &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_list_remote_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::group_image_list_finish(&iter, &images);
  }

  if (r < 0 && r != -ENOENT) {
    derr << "error listing remote group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_images.insert(m_images.end(), images.begin(), images.end());

  if (images.size() == MAX_RETURN) {
    list_remote_group();
    return;
  }

  get_remote_mirror_image();
}

template <typename I>
void BootstrapRequest<I>::get_remote_mirror_image() {
  while (!m_images.empty() &&
         m_images.front().state != cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED) {
    dout(20) << "skip " << m_images.front().spec.pool_id << " "
             << m_images.front().spec.image_id << dendl;
    m_images.pop_front();
  }

  if (m_images.empty()) {
    get_local_group_id();
    return;
  }

  auto &spec = m_images.front().spec;

  dout(10) << spec.pool_id << " " << spec.image_id << dendl;

  if (!m_pool_meta_cache->remote_pool_meta_exists(spec.pool_id)) {
    derr << "failed to find remote image pool in meta cache" << dendl;
    finish(-ENOENT);
    return;
  }

  int r = librbd::util::create_ioctx(m_remote_io_ctx, "remote image pool",
                                     spec.pool_id, {}, &m_image_io_ctx);
  if (r < 0) {
    derr << "failed to open remote image pool " << spec.pool_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, spec.image_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_get_remote_mirror_image>(this);

  r = m_image_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_remote_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  auto &spec = m_images.front().spec;
  cls::rbd::MirrorImage mirror_image;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_image_get_finish(&iter, &mirror_image);
  }

  if (r < 0 && r != -ENOENT) {
    derr << "error getting remote mirror image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_remote_images[{spec.pool_id, mirror_image.global_image_id}] = spec.image_id;

  m_images.pop_front();

  get_remote_mirror_image();
}

template <typename I>
void BootstrapRequest<I>::get_local_group_id() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_group_get_group_id_start(&op, m_global_group_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_get_local_group_id>(this);

  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_local_group_id(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  if (r == -ENOENT &&
      m_remote_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
      m_remote_mirror_group_primary) {
    ceph_assert(!m_group_name.empty());
    get_local_group_id_by_name();
    return;
  }

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_group_get_group_id_finish(
        &iter, m_local_group_id);
  }

  if (r < 0) {
    if (r != -ENOENT) {
      derr << "error getting local group id: " << cpp_strerror(r) << dendl;
    } else {
      m_local_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLED;
      r = 0;
    }
    finish(r);
    return;
  }

  get_local_group_name();
}

template <typename I>
void BootstrapRequest<I>::get_local_group_name() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, *m_local_group_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_get_local_group_name>(this);

  int r = m_local_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op,
                                      &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_local_group_name(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  std::string local_group_name;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_name_finish(&iter, &local_group_name);
  }

  if (r < 0) {
    derr << "error getting local group name: " << cpp_strerror(r) << dendl;
    if (r == -ENOENT) {
      r = -EEXIST; // split-brain
    }
    finish(r);
    return;
  }

  if (m_group_name.empty()) {
    m_group_name = local_group_name;
  } else if (m_group_name != local_group_name) {
    // should never happen
    derr << "local group name '" << local_group_name << "' does not match "
         << "remote group name '" << m_group_name << "'" << dendl;
    finish(-EEXIST); // split-brain
    return;
  }

  get_local_mirror_group();
}

template <typename I>
void BootstrapRequest<I>::get_local_group_id_by_name() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_id_start(&op, m_group_name);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_get_local_group_id_by_name>(this);

  int r = m_local_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_local_group_id_by_name(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  if (r == -ENOENT) {
    create_local_group_id();
    return;
  }

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_id_finish(&iter, m_local_group_id);
  }

  if (r < 0) {
    derr << "error getting local group id: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_local_group_id_by_name = true;
  get_local_mirror_group();
}

template <typename I>
void BootstrapRequest<I>::get_local_mirror_group() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_group_get_start(&op, *m_local_group_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_get_local_mirror_group>(this);

  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_local_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_group_get_finish(&iter,
                                                    &m_local_mirror_group);
  }

  if (r == -ENOENT) {
    if (m_local_group_id_by_name) {
      derr << "local group is not mirrored" << dendl;
      finish(-EINVAL);
      return;
    }
    if (m_remote_mirror_group.state != cls::rbd::MIRROR_GROUP_STATE_ENABLED ||
        !m_remote_mirror_group_primary) {
      derr << "can't find primary for group: " <<  m_group_name << dendl;
      finish(-EEXIST); // split-brain
      return;
    }
    r = 0;
  }

  if (r < 0) {
    derr << "error getting local mirror group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  dout(20) << m_local_mirror_group << dendl;

  list_local_group_snapshots();
}

template <typename I>
void BootstrapRequest<I>::list_local_group_snapshots() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_list_local_group_snapshots>(this);

  auto req = librbd::group::ListSnapshotsRequest<I>::create(m_local_io_ctx,
      *m_local_group_id, true, true, &local_group_snaps, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_list_local_group_snapshots(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  if (r < 0) {
    derr << "error listing local mirror group snapshots: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  for (auto it : local_group_snaps) {
    m_local_group_snaps->insert(make_pair(it.id, it));
  }

  if (m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
    cls::rbd::MirrorSnapshotState state;
    r = get_last_mirror_snapshot_state(local_group_snaps, &state);
    if (r == -ENOENT) {
      derr << "failed to find local mirror group snapshot" << dendl;
    } else {
      if (m_remote_mirror_group_primary &&
          state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED) {
        // if local snapshot is primary demoted, check if there is demote snapshot
        // in remote, if not then split brain
        if (!is_demoted_snap_exists(remote_group_snaps) && !m_resync_requested) {
          finish(-EEXIST);
          return;
        }
      }
    }
    m_local_mirror_group_primary = (state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY);
  }

  if (m_remote_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
    if (m_remote_mirror_group_primary) {
      if (m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
          m_local_mirror_group_primary) {
        derr << "both remote and local groups are primary" << dendl;
      }
    } else if (m_local_mirror_group.state != cls::rbd::MIRROR_GROUP_STATE_ENABLED ||
               !m_local_mirror_group_primary) {
      derr << "both remote and local groups are not primary" << dendl;
      finish(-EREMOTEIO);
      return;
    }
  } else if (m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
             !m_local_mirror_group_primary) {
    // trigger group removal
    m_local_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLED;
  }

  list_local_group();
}

template <typename I>
void BootstrapRequest<I>::list_local_group() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  cls::rbd::GroupImageSpec start_after;
  if (!m_images.empty()) {
    start_after = m_images.rbegin()->spec;
  }
  librbd::cls_client::group_image_list_start(&op, start_after, MAX_RETURN);
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_list_local_group>(this);
  m_out_bl.clear();
  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(*m_local_group_id), comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_list_local_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::group_image_list_finish(&iter, &images);
  }

  if (r < 0 && r != -ENOENT) {
    derr << "error listing local group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_images.insert(m_images.end(), images.begin(), images.end());

  if (images.size() == MAX_RETURN) {
    list_local_group();
    return;
  }

  get_local_mirror_image();
}

template <typename I>
void BootstrapRequest<I>::get_local_mirror_image() {
  if (m_images.empty()) {
    remove_local_image_from_group();
    return;
  }

  auto &spec = m_images.front().spec;

  dout(10) << spec.pool_id << " " << spec.image_id << dendl;

  int r = librbd::util::create_ioctx(m_local_io_ctx, "local image pool",
                                     spec.pool_id, {}, &m_image_io_ctx);
  if (r < 0) {
    derr << "failed to open local image pool " << spec.pool_id << ": "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, spec.image_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_get_local_mirror_image>(this);

  r = m_image_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_get_local_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  auto &spec = m_images.front().spec;
  cls::rbd::MirrorImage mirror_image;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_image_get_finish(&iter, &mirror_image);
  }

  if (r < 0) {
    if (r != -ENOENT) {
      derr << "error getting local mirror image: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
  } else {
    if (m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
        m_local_mirror_group_primary) {
      dout(10) << "add primary to replayer queue: " << spec.pool_id << " "
               << spec.image_id << " " << mirror_image.global_image_id
               << dendl;
      m_local_images.insert({spec.pool_id, mirror_image.global_image_id});
    } else if (m_remote_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
               m_remote_mirror_group_primary &&
               m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
               m_local_mirror_group.global_group_id == m_global_group_id &&
               has_remote_image(spec.pool_id, mirror_image.global_image_id)) {
      dout(10) << "add secondary to replayer queue: " << spec.pool_id << " "
               << spec.image_id << " " << mirror_image.global_image_id
               << dendl;
      m_local_images.insert({spec.pool_id, mirror_image.global_image_id});
    } else {
      dout(10) << "add to trash queue: " << spec.pool_id << " "
               << spec.image_id << " " << mirror_image.global_image_id
               << dendl;
      m_local_trash_images[{spec.pool_id, mirror_image.global_image_id}] =
        spec.image_id;
    }
  }

  m_images.pop_front();

  get_local_mirror_image();
}

template <typename I>
void BootstrapRequest<I>::remove_local_image_from_group() {
  if (m_local_trash_images.empty()) {
    disable_local_mirror_group();
    return;
  }

  auto &[pool_id, global_image_id] = m_local_trash_images.begin()->first;
  auto &image_id = m_local_trash_images.begin()->second;

  dout(10) << "pool_id=" << pool_id << ", image_id=" << image_id << dendl;

  int r = librbd::util::create_ioctx(m_local_io_ctx, "local image pool",
                                     pool_id, {}, &m_image_io_ctx);
  if (r < 0) {
    derr << "failed to open local image pool " << pool_id << ": "
         << cpp_strerror(r) << dendl;
    handle_remove_local_image_from_group(-ENOENT);
    return;
  }

  auto ctx = create_context_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_remove_local_image_from_group>(this);

  auto req = librbd::group::RemoveImageRequest<I>::create(
      m_local_io_ctx, *m_local_group_id, m_image_io_ctx, image_id, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_remove_local_image_from_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error removing mirror image from group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  move_local_image_to_trash();
}

template <typename I>
void BootstrapRequest<I>::move_local_image_to_trash() {
  ceph_assert(!m_local_trash_images.empty());
  auto &[pool_id, global_image_id] = m_local_trash_images.begin()->first;

  dout(10) << "pool_id=" << pool_id << ", global_image_id=" << global_image_id
           << dendl;

  int r = librbd::util::create_ioctx(m_local_io_ctx, "local image pool",
                                     pool_id, {}, &m_image_io_ctx);
  if (r < 0) {
    derr << "failed to open local image pool " << pool_id << ": "
         << cpp_strerror(r) << dendl;
    handle_move_local_image_to_trash(-ENOENT);
    return;
  }

  auto ctx = create_context_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_move_local_image_to_trash>(this);

  auto req = image_deleter::TrashMoveRequest<I>::create(
      m_image_io_ctx, global_image_id, m_resync_requested,
      m_threads->work_queue, ctx);
  req->send();
}

template <typename I>
void BootstrapRequest<I>::handle_move_local_image_to_trash(int r) {
  dout(10) << "r=" << r << dendl;

  if (m_canceled) {
    finish(-ECANCELED);
    return;
  }

  if (r < 0 && r != -ENOENT) {
    derr << "error moving mirror image to trash: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_local_trash_images.erase(m_local_trash_images.begin());

  remove_local_image_from_group();
}

template <typename I>
void BootstrapRequest<I>::disable_local_mirror_group() {
  if (m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
      m_local_mirror_group.global_group_id == m_global_group_id) {
    finish(0);
    return;
  }

  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  m_local_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLING;
  librbd::cls_client::mirror_group_set(&op, *m_local_group_id,
                                       m_local_mirror_group);

  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_disable_local_mirror_group>(this);

  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_disable_local_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error disabling local mirror group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_local_mirror_group();
}

template <typename I>
void BootstrapRequest<I>::remove_local_mirror_group() {
  if (m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
      m_local_mirror_group.global_group_id == m_global_group_id) {
    finish(0);
    return;
  }

  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_group_remove(&op, *m_local_group_id);

  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_remove_local_mirror_group>(this);

  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_remove_local_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error removing local mirror group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_local_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLED;
  if (r != -ENOENT && (m_remote_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
       m_remote_mirror_group_primary)) {
    create_local_mirror_group();
  } else {
    remove_local_group();
  }
}

template <typename I>
void BootstrapRequest<I>::remove_local_group() {
  dout(10) << m_group_name << " " << *m_local_group_id << dendl;

  ceph_assert(!m_local_group_id->empty());
  ceph_assert(!m_group_name.empty());

  librados::ObjectWriteOperation op;
  op.remove();
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_remove_local_group>(this);

  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(*m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_remove_local_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error removing local group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_local_group_id();
}

template <typename I>
void BootstrapRequest<I>::remove_local_group_id() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::group_dir_remove(&op, m_group_name, *m_local_group_id);

  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_remove_local_group_id>(this);

  int r = m_local_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_remove_local_group_id(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error removing local group id: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void BootstrapRequest<I>::create_local_group_id() {
  dout(10) << dendl;

  *m_local_group_id = librbd::util::generate_image_id(m_local_io_ctx);

  librados::ObjectWriteOperation op;
  librbd::cls_client::group_dir_add(&op, m_group_name, *m_local_group_id);

  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_create_local_group_id>(this);

  int r = m_local_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_group_id(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error creating local group id: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_local_group();
}

template <typename I>
void BootstrapRequest<I>::create_local_group() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  op.create(true);
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_create_local_group>(this);

  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(*m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error creating local group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_local_mirror_group();
}

template <typename I>
void BootstrapRequest<I>::create_local_mirror_group() {
  dout(10) << dendl;

  ceph_assert(
      m_remote_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
      m_remote_mirror_group_primary);

  librados::ObjectWriteOperation op;
  m_local_mirror_group = {m_global_group_id,
                          m_remote_mirror_group.mirror_image_mode,
                          cls::rbd::MIRROR_GROUP_STATE_ENABLED};
  librbd::cls_client::mirror_group_set(&op, *m_local_group_id,
                                       m_local_mirror_group);
  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_create_local_mirror_group>(this);

  int r = m_local_io_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_mirror_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error creating local mirror group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_local_non_primary_group_snapshot();
}

template <typename I>
void BootstrapRequest<I>::create_local_non_primary_group_snapshot() {
  dout(10) << dendl;

  RemotePoolMeta remote_pool_meta;
  int r = m_pool_meta_cache->get_remote_pool_meta(m_remote_io_ctx.get_id(),
                                                  &remote_pool_meta);
  if (r < 0 || remote_pool_meta.mirror_peer_uuid.empty()) {
    derr << "failed to retrieve mirror peer uuid from remote image pool"
         << dendl;
    finish(r < 0 ? r : -EINVAL);
    return;
  }

  librados::ObjectWriteOperation op;
  std::string group_snap_id = librbd::util::generate_image_id(m_local_io_ctx);
  cls::rbd::GroupSnapshot group_snap{
    group_snap_id,
    cls::rbd::MirrorGroupSnapshotNamespace{
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY,
      {}, remote_pool_meta.mirror_peer_uuid, {}},
      prepare_non_primary_mirror_snap_name(m_global_group_id, group_snap_id),
      cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  librbd::cls_client::group_snap_set(&op, group_snap);
  group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
  librbd::cls_client::group_snap_set(&op, group_snap);

  auto comp = create_rados_callback<
      BootstrapRequest<I>,
      &BootstrapRequest<I>::handle_create_local_non_primary_group_snapshot>(this);

  r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(*m_local_group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void BootstrapRequest<I>::handle_create_local_non_primary_group_snapshot(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error creating local non-primary group snapshot: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void BootstrapRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(r != -ENOENT);
  if (r == 0) {
    if (m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_DISABLED) {
      r = -ENOENT;
    } else {
      *m_local_group_ctx = {m_group_name, *m_local_group_id, m_global_group_id,
                            m_local_mirror_group_primary, m_local_io_ctx};
      r = create_replayers();
    }
  }

  m_on_finish->complete(r);
}

template <typename I>
int BootstrapRequest<I>::create_replayers() {
  dout(10) << dendl;

  int r = 0;
  if (m_remote_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
      m_remote_mirror_group_primary) {
    for (auto &[p, remote_image_id] : m_remote_images) {
      auto &remote_pool_id = p.first;
      auto &global_image_id = p.second;

      m_image_replayers->emplace_back(librados::IoCtx(), nullptr);
      auto &local_io_ctx = m_image_replayers->back().first;
      auto &image_replayer = m_image_replayers->back().second;

      RemotePoolMeta remote_pool_meta;
      r = m_pool_meta_cache->get_remote_pool_meta(remote_pool_id,
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

      (*m_image_replayer_index)[{remote_pool_id, remote_image_id}] = image_replayer;
    }
  } else if (m_local_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED &&
             m_local_mirror_group_primary) {
    for (auto &[local_pool_id, global_image_id] : m_local_images) {
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

      int64_t remote_pool_id = librados::Rados(m_remote_io_ctx).pool_lookup(
          local_io_ctx.get_pool_name().c_str());

      RemotePoolMeta remote_pool_meta;
      r = m_pool_meta_cache->get_remote_pool_meta(remote_pool_id,
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
  } else {
    ceph_abort();
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

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::BootstrapRequest<librbd::ImageCtx>;
