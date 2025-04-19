// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "tools/rbd_mirror/group_replayer/PrepareRemoteGroupRequest.h"
#include "tools/rbd_mirror/group_replayer/GroupStateBuilder.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_client.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/mirror/GroupGetInfoRequest.h"
#include "tools/rbd_mirror/Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::" \
                           << "PrepareRemoteGroupRequest: " << this << " " \
                           << __func__ << ": "

namespace {
  static const uint32_t MAX_RETURN = 1024;
} // anonymous namespace

namespace rbd {
namespace mirror {
namespace group_replayer {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void PrepareRemoteGroupRequest<I>::send() {
  dout(10) << "global_group_id: " << m_global_group_id << dendl;
  get_remote_group_id();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::get_remote_group_id() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_group_get_group_id_start(&op, m_global_group_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      PrepareRemoteGroupRequest<I>,
      &PrepareRemoteGroupRequest<I>::handle_get_remote_group_id>(this);

  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::handle_get_remote_group_id(int r) {
  dout(10) << "r=" << r << ", global_image_id: " << m_global_group_id << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_group_get_group_id_finish(
        &iter, &m_remote_group_id);
  }

  if (r < 0) {
    derr << "error getting remote group id: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_remote_group_name();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::get_remote_group_name() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, m_remote_group_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      PrepareRemoteGroupRequest<I>,
      &PrepareRemoteGroupRequest<I>::handle_get_remote_group_name>(this);

  int r = m_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op,
                                      &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::handle_get_remote_group_name(int r) {
  dout(10) << "r=" << r << dendl;

  *m_remote_group_name = "";
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_name_finish(&iter, m_remote_group_name);
  }

  if (r < 0) {
    derr << "error getting remote group name: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_mirror_info();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::get_mirror_info() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    PrepareRemoteGroupRequest<I>,
    &PrepareRemoteGroupRequest<I>::handle_get_mirror_info>(this);
  auto req = librbd::mirror::GroupGetInfoRequest<I>::create(
    m_io_ctx, "", m_remote_group_id, &m_mirror_group,
    &m_promotion_state, ctx);
  req->send();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::handle_get_mirror_info(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    if (r == -ENOENT) {
      dout(10) << "group " << m_global_group_id << " is not mirrored" << dendl;
    } else {
      derr << "failed to retrieve remote mirror group info: " << cpp_strerror(r)
	   << dendl;
    }
    finish(r);
    return;
  }

 if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_DISABLING) {
    dout(10) << "remote group mirroring is being disabled" << dendl;
    finish(-ERESTART);
    return;
  }

  if (m_mirror_group.mirror_image_mode !=
      cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    derr << "unsupported mirror group mode "
         << m_mirror_group.mirror_image_mode << " "
         << "for group " << m_global_group_id << dendl;
    finish(-EOPNOTSUPP);
    return;
  }

  dout(10) << "remote_group_id=" << m_remote_group_id << ", "
           << "remote_promotion_state=" << m_promotion_state
           << dendl;

  list_group_images();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::list_group_images() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  cls::rbd::GroupImageSpec start_after;

  m_out_bl.clear();

  if (!m_images.empty()) {
    start_after = m_images.rbegin()->spec;
  }

  librbd::cls_client::group_image_list_start(&op, start_after, MAX_RETURN);
  auto comp = create_rados_callback<
      PrepareRemoteGroupRequest<I>,
      &PrepareRemoteGroupRequest<I>::handle_list_group_images>(this);
  int r = m_io_ctx.aio_operate(
      librbd::util::group_header_name(m_remote_group_id), comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::handle_list_group_images(int r) {
  dout(10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::group_image_list_finish(&iter, &images);
  }

  if (r < 0) {
    derr << "error listing remote group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_images.insert(m_images.end(), images.begin(), images.end());

  if (images.size() == MAX_RETURN) {
    list_group_images();
    return;
  }
  dout(10) << "number of images: " << m_images.size() << dendl;
  get_mirror_images();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::get_mirror_images() {
  dout(10) << dendl;

  if (m_images.empty()) {
    finish(0);
    return;
  }

  auto &spec = m_images.front().spec;

  dout(10) << "pool_id: " << spec.pool_id
           << ", image_id " << spec.image_id << dendl;

  // Currently the group and its images must belong to the same pool.

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, spec.image_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      PrepareRemoteGroupRequest<I>,
      &PrepareRemoteGroupRequest<I>::handle_get_mirror_images>(this);

  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::handle_get_mirror_images(int r) {
  dout(10) << "r=" << r << dendl;

 auto &spec = m_images.front().spec;
  cls::rbd::MirrorImage mirror_image;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_image_get_finish(&iter, &mirror_image);
  }

  if (r < 0) {
    derr << "error getting local mirror image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_remote_images.insert({spec.pool_id, mirror_image.global_image_id});

  m_images.pop_front();
  get_mirror_images();
}

template <typename I>
void PrepareRemoteGroupRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == 0) {
    (*m_state_builder)->remote_group_id = m_remote_group_id;
    (*m_state_builder)->group_name = *m_remote_group_name;
    (*m_state_builder)->remote_promotion_state = m_promotion_state;
    (*m_state_builder)->remote_images.insert(m_remote_images.begin(),
                                             m_remote_images.end());
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::PrepareRemoteGroupRequest<librbd::ImageCtx>;

