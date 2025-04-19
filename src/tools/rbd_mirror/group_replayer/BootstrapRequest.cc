// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
    const std::string &local_mirror_uuid, // FIXME: Not used
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
  } else if (r == -ENOENT) {
    if (state_builder->remote_group_id.empty()) {
      if (state_builder->local_group_id.empty()) {
	// Neither group exists
	m_local_group_removed = true; //FIXME
	finish(0);
	return;
      } else if (state_builder->local_promotion_state ==
                 librbd::mirror::PROMOTION_STATE_NON_PRIMARY){
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
      finish(-EINVAL);
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

  ceph_assert(!(*m_state_builder)->local_group_id.empty());

  auto ctx = create_context_callback<
    BootstrapRequest,
    &BootstrapRequest<I>::handle_remove_local_group>(this);

  auto req = RemoveLocalGroupRequest<I>::create(
    m_local_io_ctx, m_global_group_id, *m_resync_requested,
    m_threads->work_queue, ctx);
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
      r = create_replayers();
    }
  }

  m_on_finish->complete(r);
}

template <typename I>
int BootstrapRequest<I>::create_replayers() {
  dout(10) << dendl;

  //TODO: check that the images have not changed
  if (!m_image_replayers->empty()) {
    dout(10) << "image replayers already exist."<< dendl;
    return 0;
  }

  auto state_builder = *m_state_builder;
  int r = 0;

  if ((*m_state_builder)->is_local_primary()) {
  // The ImageReplayers are required to run even when the group is primary in
  // order to update the image status for the mirror pool status to be healthy.
    for (auto &[global_image_id, p] : (*m_state_builder)->local_images) {
      auto &local_pool_id = p.first;

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
  } else if (!state_builder->remote_group_id.empty()) {
    for (auto &[remote_pool_id, global_image_id] : (*m_state_builder)->remote_images) {

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
