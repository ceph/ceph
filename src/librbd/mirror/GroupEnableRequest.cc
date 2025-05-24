// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupEnableRequest.h"
#include "librbd/mirror/GetMirrorImageRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"
#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupEnableRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace {

const uint32_t MAX_RETURN = 1024;

} // anonymous namespace


using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
GroupEnableRequest<I>::GroupEnableRequest(librados::IoCtx &io_ctx,
                                const std::string &group_id,
                                cls::rbd::MirrorImageMode mode,
                                Context *on_finish)
  : m_io_ctx(io_ctx), m_group_id(group_id), m_mode(mode),
    m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())) {
}

template <typename I>
void GroupEnableRequest<I>::send() {
  get_mirror_group();
}

template <typename I>
void GroupEnableRequest<I>::get_mirror_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_group_get_start(&op, m_group_id);

  using klass = GroupEnableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_mirror_group>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_get_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_group_get_finish(&iter, &m_mirror_group);
  }
/*
  if (r == 0 && m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_CREATING &&
      !m_non_primary_global_image_id.empty()) {
    // special case where rbd-mirror injects a disabled record to record the
    // local image id prior to creating ther image
    ldout(m_cct, 10) << "enabling mirroring on in-progress image replication"
                     << dendl;
  } else */
  if (r == 0) {
    if (m_mirror_group.mirror_image_mode != m_mode) {
      lderr(m_cct) << "invalid current group mirror mode" << dendl;
      r = -EINVAL;
    } else if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
      ldout(m_cct, 10) << "mirroring is already enabled" << dendl;
    } else if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLING) {
      ldout(m_cct, 10) << "mirroring either in progress or was interrupted"
                       << dendl;
      r = -EINVAL;
    } else {
      lderr(m_cct) << "currently disabling" << dendl;
      r = -EINVAL;
    }
    finish(r);
    return;
  } else if (r != -ENOENT) {
    lderr(m_cct) << "failed to retrieve mirror image: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  get_mirror_peer_list();
}

template <typename I>
void GroupEnableRequest<I>::get_mirror_peer_list() {
  ldout(m_cct, 10) << dendl;

  m_default_ns_ioctx.dup(m_io_ctx);
  m_default_ns_ioctx.set_namespace("");

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  auto comp = create_rados_callback<
      GroupEnableRequest<I>,
      &GroupEnableRequest<I>::handle_get_mirror_peer_list>(this);

  m_out_bl.clear();
  int r = m_default_ns_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_get_mirror_peer_list(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
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
void GroupEnableRequest<I>::list_group_images() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_image_list_start(&op, m_start_after, MAX_RETURN);

  auto comp = create_rados_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_list_group_images>(this);

  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(
    librbd::util::group_header_name(m_group_id), comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupEnableRequest<I>::handle_list_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
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

  get_mirror_images();
}

template <typename I>
void GroupEnableRequest<I>::get_mirror_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>,
    &GroupEnableRequest<I>::handle_get_mirror_images>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  size_t i = 0;
  m_mirror_images.resize(m_images.size());
  for (i = 0; i < m_images.size(); i++) {
    auto image_spec = m_images[i].spec;
    auto req = GetMirrorImageRequest<I>::create(m_io_ctx, image_spec.image_id,
                                                &m_mirror_images[i],
                                                gather_ctx->new_sub());
    req->send();
  }
  gather_ctx->activate();
}

template <typename I>
void GroupEnableRequest<I>::handle_get_mirror_images(int r) {
  ldout(m_cct, 10) << "r=" << r <<  dendl;
  if (r < 0) {
    ldout(m_cct, 10) << "failed to get mirror image info" << dendl;
    finish(r);
    return;
  }

  for (size_t i = 0; i < m_mirror_images.size(); i++) {
    if (m_mirror_images[i].state != cls::rbd::MIRROR_IMAGE_STATE_DISABLED) {
      lderr(m_cct) << "image is already mirror enabled: "
                   << m_images[i].spec.image_id << dendl;
      finish(-EINVAL); 
      return;
    }
  }

  open_images();
}

template <typename I>
void GroupEnableRequest<I>::open_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>, &GroupEnableRequest<I>::handle_open_images>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);
  int r = 0;
  size_t i = 0;

  for ( ; i < m_images.size(); i++) {
    auto &image = m_images[i];
    librbd::IoCtx image_io_ctx;
    r = librbd::util::create_ioctx(m_io_ctx, "image",
                                   image.spec.pool_id, {},
                                   &image_io_ctx);
    if (r < 0) {
      m_ret_val = r;
      break;
    }

    librbd::ImageCtx* image_ctx = new ImageCtx("", image.spec.image_id.c_str(),
					       nullptr, image_io_ctx, false);

/*
    image_ctx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT |
                           OPEN_FLAG_IGNORE_MIGRATING, gather_ctx->new_sub());
*/
    // Open parent as well as we need to check if the image is a clone
    image_ctx->state->open(0, gather_ctx->new_sub());

    m_image_ctxs.push_back(image_ctx);
  }

  gather_ctx->activate();
}

template <typename I>
void GroupEnableRequest<I>::handle_open_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && m_ret_val == 0) {
    m_ret_val = r;
  }

  if (m_ret_val < 0) {
    lderr(m_cct) << "failed to open group images: " << cpp_strerror(m_ret_val)
                 << dendl;
    close_images();
    return;
  }

  validate_images();
}

template <typename I>
void GroupEnableRequest<I>::validate_images() {

  for (auto &image_ctx : m_image_ctxs) {
    std::shared_lock image_locker{image_ctx->image_lock};
    if (image_ctx->parent) {
      lderr(m_cct) << "cannot mirror enable group: cloned images are not supported"
                   << dendl;
      m_ret_val = -EINVAL;
      close_images();
      return;
    }
  }
  finish(0);
}

template <typename I>
void GroupEnableRequest<I>::close_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupEnableRequest<I>, &GroupEnableRequest<I>::handle_close_images>(this);

  auto gather_ctx = new C_Gather(m_cct, ctx);

  for (auto ictx: m_image_ctxs) {
    ictx->state->close(gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupEnableRequest<I>::handle_close_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to close image: " << cpp_strerror(r) << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }

  if (m_ret_val < 0) {
    finish(m_ret_val);
    return;
  }
}

template <typename I>
void GroupEnableRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupEnableRequest<librbd::ImageCtx>;
