// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "tools/rbd_mirror/group_replayer/PrepareLocalGroupRequest.h"
#include "tools/rbd_mirror/group_replayer/GroupStateBuilder.h"
#include "tools/rbd_mirror/group_replayer/RemoveLocalGroupRequest.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_client.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/mirror/GroupGetInfoRequest.h"
#include "tools/rbd_mirror/Threads.h"
#include <type_traits>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::group_replayer::" \
                           << "PrepareLocalGroupRequest: " << this << " " \
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
void PrepareLocalGroupRequest<I>::send() {
  dout(10) << "global_group_id: " << m_global_group_id << dendl;

  get_local_group_id();
}

template <typename I>
void PrepareLocalGroupRequest<I>::get_local_group_id() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_group_get_group_id_start(&op, m_global_group_id);
  m_out_bl.clear();
  auto comp = create_rados_callback<
      PrepareLocalGroupRequest<I>,
      &PrepareLocalGroupRequest<I>::handle_get_local_group_id>(this);

  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void PrepareLocalGroupRequest<I>::handle_get_local_group_id(int r) {
  dout(10) << "r=" << r << ", global_group_id: " << m_global_group_id << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_group_get_group_id_finish(
        &iter, &m_local_group_id);
  }

  if (r < 0) {
    if (r != -ENOENT) {
      derr << "error getting local group id: " << cpp_strerror(r) << dendl;
    }
    finish(r);
    return;
  }

  get_local_group_name();
}

template <typename I>
void PrepareLocalGroupRequest<I>::get_local_group_name() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, m_local_group_id);

  m_out_bl.clear();
  auto comp = create_rados_callback<
      PrepareLocalGroupRequest<I>,
      &PrepareLocalGroupRequest<I>::handle_get_local_group_name>(this);

  int r = m_io_ctx.aio_operate(RBD_GROUP_DIRECTORY, comp, &op,
                                      &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void PrepareLocalGroupRequest<I>::handle_get_local_group_name(int r) {
  dout(10) << "r=" << r << dendl;

  *m_local_group_name = "";
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_name_finish(&iter, m_local_group_name);
  }

  if (r == -ENOENT) {
    // proceed - we should have a mirror group record if we got this far
    dout(10) << "group does not exist for local group id " << m_local_group_id
             << dendl;
    *m_local_group_name = "";
  } else if (r < 0) {
    derr << "error getting local group name: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_mirror_info();
}

template <typename I>
void PrepareLocalGroupRequest<I>::get_mirror_info() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    PrepareLocalGroupRequest<I>,
    &PrepareLocalGroupRequest<I>::handle_get_mirror_info>(this);

  auto req = librbd::mirror::GroupGetInfoRequest<I>::create(
    m_io_ctx, "", m_local_group_id, &m_mirror_group,
    &m_promotion_state, ctx);
  req->send();
}

template <typename I>
void PrepareLocalGroupRequest<I>::handle_get_mirror_info(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to retrieve local mirror group info: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  // If the mirror group state is set to CREATING, it means that the group
  // creation was interrupted.
  if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_CREATING) {
    dout(10) << "local group is still in creating state, issuing a removal"
            << dendl;
    remove_local_group();
    return;
  } else if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_DISABLING) {
    dout(10) << "local group mirroring is in disabling state" << dendl;

    finish(-ERESTART);
    return;
  }

  if (m_mirror_group.mirror_image_mode != cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    derr << "unsupported mirror mode "
         << m_mirror_group.mirror_image_mode << " "
         << "for group " << m_global_group_id << dendl;
    finish(-EOPNOTSUPP);
    return;
  }

  list_group_images();
}

template <typename I>
void PrepareLocalGroupRequest<I>::list_group_images() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  cls::rbd::GroupImageSpec start_after;

  m_out_bl.clear();

  if (!m_images.empty()) {
    start_after = m_images.rbegin()->spec;
  }

  librbd::cls_client::group_image_list_start(&op, start_after, MAX_RETURN);
  auto comp = create_rados_callback<
      PrepareLocalGroupRequest<I>,
      &PrepareLocalGroupRequest<I>::handle_list_group_images>(this);
  int r = m_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_id), comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void PrepareLocalGroupRequest<I>::handle_list_group_images(int r) {
  dout(10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::group_image_list_finish(&iter, &images);
  }

  if (r < 0) {
    dout(10) << "error listing local group: " << cpp_strerror(r) << dendl;
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
void PrepareLocalGroupRequest<I>::get_mirror_images() {
  dout(10) << dendl;

  if (m_images.empty()) {
    dout(10) << "local_group_id=" << m_local_group_id << ", "
             << "local_promotion_state=" << m_promotion_state
	     << dendl;

    (*m_state_builder)->local_group_id = m_local_group_id;
    (*m_state_builder)->local_promotion_state = m_promotion_state;
    (*m_state_builder)->group_name = *m_local_group_name; //FIXME
    (*m_state_builder)->local_images.insert(m_local_images.begin(),
                                             m_local_images.end());
    finish(0);
    return;
  }

  auto &spec = m_images.front().spec;

  dout(10) << "pool_id: " << spec.pool_id
           << ", image_id " << spec.image_id << dendl;

  // FIXME: Currently images must be in the same pool as the group
  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, spec.image_id);

  m_out_bl.clear();
  auto comp = create_rados_callback<
      PrepareLocalGroupRequest<I>,
      &PrepareLocalGroupRequest<I>::handle_get_mirror_images>(this);

  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void PrepareLocalGroupRequest<I>::handle_get_mirror_images(int r) {
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
  m_local_images.insert({mirror_image.global_image_id,
                         {spec.pool_id, spec.image_id}});

  m_images.pop_front();
  get_mirror_images();
}

template <typename I>
void PrepareLocalGroupRequest<I>::remove_local_group() {
  dout(10) << "group_name: " << *m_local_group_name
           << " , group_id: " << m_local_group_id << dendl;

  auto ctx = create_context_callback<
    PrepareLocalGroupRequest,
    &PrepareLocalGroupRequest<I>::handle_remove_local_group>(this);

  auto req = RemoveLocalGroupRequest<I>::create(m_io_ctx, m_global_group_id,
                                                false, m_work_queue, ctx);
  req->send();
}

template <typename I>
void PrepareLocalGroupRequest<I>::handle_remove_local_group(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    derr << "error removing local group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(-ENOENT);
}

template <typename I>
void PrepareLocalGroupRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::group_replayer::PrepareLocalGroupRequest<librbd::ImageCtx>;

