// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "librbd/Utils.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"

#include "UpdateRequest.h"
#include "LoadRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::LoadRequest: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

static const uint32_t MAX_RETURN = 1024;

using librbd::util::create_rados_callback;
using librbd::util::create_context_callback;

template<typename I>
LoadRequest<I>::LoadRequest(librados::IoCtx &ioctx,
                            std::map<std::string, cls::rbd::MirrorImageMap> *image_mapping,
                            Context *on_finish)
  : m_ioctx(ioctx),
    m_image_mapping(image_mapping),
    m_on_finish(on_finish) {
}

template<typename I>
void LoadRequest<I>::send() {
  dout(20) << dendl;

  image_map_list();
}

template<typename I>
void LoadRequest<I>::image_map_list() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_map_list_start(&op, m_start_after, MAX_RETURN);

  librados::AioCompletion *aio_comp = create_rados_callback<
    LoadRequest, &LoadRequest::handle_image_map_list>(this);

  m_out_bl.clear();
  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template<typename I>
void LoadRequest<I>::handle_image_map_list(int r) {
  dout(20) << ": r=" << r << dendl;

  std::map<std::string, cls::rbd::MirrorImageMap> image_mapping;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_image_map_list_finish(&it, &image_mapping);
  }

  if (r < 0) {
    derr << ": failed to get image map: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_image_mapping->insert(image_mapping.begin(), image_mapping.end());

  if (image_mapping.size() == MAX_RETURN) {
    m_start_after = image_mapping.rbegin()->first;
    image_map_list();
    return;
  }

  mirror_image_list();
}

template<typename I>
void LoadRequest<I>::mirror_image_list() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_list_start(&op, m_start_after, MAX_RETURN);

  m_out_bl.clear();
  librados::AioCompletion *aio_comp = create_rados_callback<
    LoadRequest<I>,
    &LoadRequest<I>::handle_mirror_image_list>(this);
  int r = m_ioctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template<typename I>
void LoadRequest<I>::handle_mirror_image_list(int r) {
  dout(20) << ": r=" << r << dendl;

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

  for (auto &id : ids) {
    m_global_image_ids.emplace(id.second);
  }

  if (ids.size() == MAX_RETURN) {
    m_start_after = ids.rbegin()->first;
    mirror_image_list();
    return;
  }

  cleanup_image_map();
}

template<typename I>
void LoadRequest<I>::cleanup_image_map() {
  dout(20) << dendl;

  std::set<std::string> map_removals;

  auto it = m_image_mapping->begin();
  while (it != m_image_mapping->end()) {
    if (m_global_image_ids.count(it->first) > 0) {
      ++it;
      continue;
    }
    map_removals.emplace(it->first);
    it = m_image_mapping->erase(it);
  }

  if (map_removals.size() == 0) {
    finish(0);
    return;
  }

  auto ctx = create_context_callback<
     LoadRequest<I>,
     &LoadRequest<I>::finish>(this);
  image_map::UpdateRequest<I> *req = image_map::UpdateRequest<I>::create(
    m_ioctx, {}, std::move(map_removals), ctx);
  req->send();
}

template<typename I>
void LoadRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::LoadRequest<librbd::ImageCtx>;
