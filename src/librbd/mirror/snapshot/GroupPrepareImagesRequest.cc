// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "include/ceph_assert.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/Context.h"
#include "librbd/api/Group.h"
#include "librbd/internal.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/Utils.h"
#include "librbd/mirror/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::GroupPrepareImagesRequest: " \
                           << " " << __func__ << ": "
namespace librbd {
namespace mirror {
namespace snapshot {

namespace {

const uint32_t MAX_RETURN = 1024;

} // anonymous namespace

using librbd::util::create_rados_callback;

template <typename I>
GroupPrepareImagesRequest<I>::GroupPrepareImagesRequest(
    librados::IoCtx& group_ioctx, const std::string& group_id,
    cls::rbd::MirrorSnapshotState state, uint32_t flags,
    cls::rbd::GroupSnapshot *group_snap,
    std::vector<librbd::ImageCtx *> *image_ctxs,
    std::vector<uint64_t> *quiesce_requests,
    Context *on_finish)
     : m_group_ioctx(group_ioctx), m_group_id(group_id),
       m_state(state), m_flags(flags), m_group_snap(group_snap),
       m_image_ctxs(image_ctxs), m_quiesce_requests(quiesce_requests),
       m_on_finish(on_finish) {
  m_cct = reinterpret_cast<CephContext*>(m_group_ioctx.cct());
  ldout(m_cct, 10) << "group_id=" << m_group_id
                   << ", state=" << m_state
                   << ", flags=" << m_flags
                   << dendl;
}

template <typename I>
void GroupPrepareImagesRequest<I>::send() {
 ldout(m_cct, 10) << dendl;

 check_snap_create_flags();
}

template <typename I>
void GroupPrepareImagesRequest<I>::check_snap_create_flags() {
  ldout(m_cct, 10) << dendl;

  int r = librbd::util::snap_create_flags_api_to_internal(m_cct, m_flags,
                                                          &m_internal_flags);
  if (r < 0) {
    lderr(m_cct) << "checking snap create flags failed" << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  get_mirror_peer_list();
}

template <typename I>
void GroupPrepareImagesRequest<I>::get_mirror_peer_list() {
  ldout(m_cct, 10) << dendl;

  m_default_ns_ioctx.dup(m_group_ioctx);
  m_default_ns_ioctx.set_namespace("");

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  auto comp = create_rados_callback<
      GroupPrepareImagesRequest<I>,
      &GroupPrepareImagesRequest<I>::handle_get_mirror_peer_list>(this);

  m_outbl.clear();
  int r = m_default_ns_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_get_mirror_peer_list(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto it = m_outbl.cbegin();
    r = cls_client::mirror_peer_list_finish(&it, &peers);
  }

  if (r < 0) {
    lderr(m_cct) << "error listing mirror peers" << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  for (auto &peer : peers) {
    if (peer.mirror_peer_direction == cls::rbd::MIRROR_PEER_DIRECTION_RX) {
      continue;
    }
    m_mirror_peer_uuids.insert(peer.uuid);
  }

  if (m_mirror_peer_uuids.empty()) {
    lderr(m_cct) << "no mirror tx peers configured for the pool" << dendl;
    finish(-EINVAL);
    return;
  }

  list_group_images();
}

template <typename I>
void GroupPrepareImagesRequest<I>::list_group_images() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_image_list_start(&op, m_start_after, MAX_RETURN);

  auto comp = create_rados_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_list_group_images>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(
    librbd::util::group_header_name(m_group_id), comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_list_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_outbl.cbegin();
    r = cls_client::group_image_list_finish(&iter, &images);
  }

  if (r < 0) {
    lderr(m_cct) << "error listing images in group: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  auto image_count = images.size();
  m_images.insert(m_images.end(), images.begin(), images.end());
  if (image_count == MAX_RETURN) {
    m_start_after = images.rbegin()->spec;
    list_group_images();
    return;
  }

  open_group_images();
}

template <typename I>
void GroupPrepareImagesRequest<I>::open_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_open_group_images>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  int r = 0;
  size_t i = 0;
  for ( ; i < m_images.size(); i++) {
    auto &image = m_images[i];
    librbd::IoCtx image_io_ctx;
    r = librbd::util::create_ioctx(m_group_ioctx, "image",
                                   image.spec.pool_id, {},
                                   &image_io_ctx);
    if (r < 0) {
      m_ret_code = r;
      break;
    }

    librbd::ImageCtx* image_ctx = new ImageCtx("", image.spec.image_id.c_str(),
					       nullptr, image_io_ctx, false);

    image_ctx->state->open(0, gather_ctx->new_sub());

    m_image_ctxs->push_back(image_ctx);
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_open_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && m_ret_code == 0) {
    m_ret_code = r;
  }

  if (m_ret_code < 0) {
    lderr(m_cct) << "failed to open group images: " << cpp_strerror(m_ret_code)
                 << dendl;
    close_images();
    return;
  }

  set_snap_metadata();
}

template <typename I>
void GroupPrepareImagesRequest<I>::set_snap_metadata() {
  ldout(m_cct, 10) << dendl;

  m_group_snap->snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    m_state, m_mirror_peer_uuids, {}, {}};

  for (auto image_ctx: *m_image_ctxs) {
    m_group_snap->snaps.emplace_back(image_ctx->md_ctx.get_id(), image_ctx->id,
                                     CEPH_NOSNAP);
  }

  std::string group_header_oid = librbd::util::group_header_name(m_group_id);
  int r = cls_client::group_snap_set(&m_group_ioctx, group_header_oid,
                                     *m_group_snap);
  if (r < 0) {
    lderr(m_cct) << "failed to set group snapshot metadata: " << cpp_strerror(r)
                 << dendl;
    m_ret_code = r;
    close_images();
    return;
  }

  if (m_image_ctxs->empty()) {
    finish(0);
    return;
  }

  if ((m_internal_flags & SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE) == 0) {
    notify_quiesce();
  } else {
    acquire_image_exclusive_locks();
  }
}

template <typename I>
void GroupPrepareImagesRequest<I>::notify_quiesce() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_notify_quiesce>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  int image_count = m_image_ctxs->size();
  m_quiesce_requests->resize(image_count);
  for (int i = 0; i < image_count; ++i) {
    auto ictx = (*m_image_ctxs)[i];
    ictx->image_watcher->notify_quiesce(&(*m_quiesce_requests)[i], m_prog_ctx,
                                       gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_notify_quiesce(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 &&
      (m_internal_flags & SNAP_CREATE_FLAG_IGNORE_NOTIFY_QUIESCE_ERROR) == 0) {
    m_ret_code = r;
    notify_unquiesce();
    return;
  }

  acquire_image_exclusive_locks();
}

template <typename I>
void GroupPrepareImagesRequest<I>::acquire_image_exclusive_locks() {
  ldout(m_cct, 10) << dendl;

  auto ctx = librbd::util::create_context_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_acquire_image_exclusive_locks>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: *m_image_ctxs) {
    std::shared_lock owner_lock{ictx->owner_lock};
    if (ictx->exclusive_lock != nullptr) {
      ictx->exclusive_lock->block_requests(-EBUSY);
      ictx->exclusive_lock->acquire_lock(gather_ctx->new_sub());
    }
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_acquire_image_exclusive_locks(
    int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to acquire image exclusive locks: "
                 << cpp_strerror(r) << dendl;
    m_ret_code = r;
    if (!m_quiesce_requests->empty()) {
      notify_unquiesce();
    } else {
      remove_snap_metadata();
    }
    return;
  }

  finish(0);
}

template <typename I>
void GroupPrepareImagesRequest<I>::notify_unquiesce() {
  ldout(m_cct, 10) << dendl;

  ceph_assert(m_quiesce_requests->size() == m_image_ctxs->size());

  auto ctx = librbd::util::create_context_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_notify_unquiesce>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  int image_count = m_image_ctxs->size();
  for (int i = 0; i < image_count; ++i) {
    auto ictx = (*m_image_ctxs)[i];
    ictx->image_watcher->notify_unquiesce((*m_quiesce_requests)[i],
                                          gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_notify_unquiesce(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    // ignore error
    lderr(m_cct) << "failed to notify the unquiesce requests: "
                 << cpp_strerror(r) << dendl;
  }

  remove_snap_metadata();
}

template <typename I>
void GroupPrepareImagesRequest<I>::remove_snap_metadata() {
  ldout(m_cct, 10) << dendl;

  int r = cls_client::group_snap_remove(
    &m_group_ioctx, librbd::util::group_header_name(m_group_id),
    m_group_snap->id);
  if (r < 0) {
    // ignore error
    lderr(m_cct) << "failed to remove group snapshot metadata: "
                 << cpp_strerror(r) << dendl;
  }

  close_images();
}

template <typename I>
void GroupPrepareImagesRequest<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  if (m_image_ctxs->empty()) {
    finish(m_ret_code);
    return;
  }

  auto ctx = librbd::util::create_context_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_close_images>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: *m_image_ctxs) {
    ictx->state->close(gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    //ignore error
    lderr(m_cct) << "failed to close images: " << cpp_strerror(r) << dendl;
  }

  m_image_ctxs->clear();
  finish(m_ret_code);
}

template <typename I>
void GroupPrepareImagesRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::GroupPrepareImagesRequest<librbd::ImageCtx>;
