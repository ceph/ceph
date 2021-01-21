// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/pool_watcher/RefreshEntitiesRequest.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"
#include <map>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::pool_watcher::RefreshEntitiesRequest " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace pool_watcher {

static const uint32_t MAX_RETURN = 1024;

using librbd::util::create_rados_callback;

template <typename I>
void RefreshEntitiesRequest<I>::send() {
  m_entities->clear();
  mirror_image_list();
}

template <typename I>
void RefreshEntitiesRequest<I>::mirror_image_list() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_list_start(&op, m_start_after, MAX_RETURN,
                                              false);
  m_out_bl.clear();
  librados::AioCompletion *aio_comp = create_rados_callback<
    RefreshEntitiesRequest<I>,
    &RefreshEntitiesRequest<I>::handle_mirror_image_list>(this);
  int r = m_remote_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RefreshEntitiesRequest<I>::handle_mirror_image_list(int r) {
  dout(10) << "r=" << r << dendl;

  std::map<std::string, std::string> ids;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_image_list_finish(&it, &ids);
  }

  if (r < 0 && r != -ENOENT) {
    derr << "failed to list mirrored images: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  for (auto &[image_id, global_image_id] : ids) {
    m_entities->insert(
        {{MIRROR_ENTITY_TYPE_IMAGE, global_image_id, 1}, image_id});
  }

  if (ids.size() == MAX_RETURN) {
    m_start_after = ids.rbegin()->first;
    mirror_image_list();
    return;
  }

  m_start_after = {};
  mirror_group_list();
}

template <typename I>
void RefreshEntitiesRequest<I>::mirror_group_list() {
  dout(10) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_group_list_start(&op, m_start_after, MAX_RETURN);

  m_out_bl.clear();
  librados::AioCompletion *aio_comp = create_rados_callback<
    RefreshEntitiesRequest<I>,
    &RefreshEntitiesRequest<I>::handle_mirror_group_list>(this);
  int r = m_remote_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RefreshEntitiesRequest<I>::handle_mirror_group_list(int r) {
  dout(10) << "r=" << r << dendl;

  std::map<std::string, cls::rbd::MirrorGroup> groups;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_group_list_finish(&it, &groups);
  }

  if (r < 0 && r != -ENOENT) {
    derr << "failed to list mirrored groups: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_groups.insert(groups.begin(), groups.end());

  if (groups.size() == MAX_RETURN) {
    m_start_after = groups.rbegin()->first;
    mirror_group_list();
    return;
  }

  // XXXMG: should we just provide group_size field in cls::rbd::MirrorGroup
  // instead of listing group images just to learn their count?
  group_image_list();
}

template <typename I>
void RefreshEntitiesRequest<I>::group_image_list() {
  if (m_groups.empty()) {
    finish(0);
    return;
  }

  auto &group_id = m_groups.begin()->first;

  dout(10) << group_id << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::group_image_list_start(
      &op, m_start_group_image_list_after, MAX_RETURN);
  m_out_bl.clear();
  librados::AioCompletion *aio_comp = create_rados_callback<
    RefreshEntitiesRequest<I>,
    &RefreshEntitiesRequest<I>::handle_group_image_list>(this);
  int r = m_remote_io_ctx.aio_operate(librbd::util::group_header_name(group_id),
                                      aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RefreshEntitiesRequest<I>::handle_group_image_list(int r) {
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

  auto image_count = images.size();
  m_group_size += image_count;
  if (image_count == MAX_RETURN) {
    m_start_group_image_list_after = images.rbegin()->spec;
  } else {
    auto group_it = m_groups.begin();
    auto &global_group_id = group_it->second.global_group_id;
    m_entities->insert(
      {{MIRROR_ENTITY_TYPE_GROUP, global_group_id, m_group_size},
       group_it->first});
    m_groups.erase(group_it);
    m_start_group_image_list_after = {};
    m_group_size = 0;
  }

  group_image_list();
}

template <typename I>
void RefreshEntitiesRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace pool_watcher
} // namespace mirror
} // namespace rbd

template class rbd::mirror::pool_watcher::RefreshEntitiesRequest<librbd::ImageCtx>;
