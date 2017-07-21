// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "librbd/Utils.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"

#include "UpdateRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::UpdateRequest: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::create_rados_callback;

static const uint32_t MAX_UPDATE = 256;

template <typename I>
UpdateRequest<I>::UpdateRequest(librados::IoCtx &ioctx,
                                std::map<std::string, cls::rbd::MirrorImageMap> &&update_mapping,
                                std::set<std::string> &&remove_global_image_ids, Context *on_finish)
  : m_ioctx(ioctx),
    m_update_mapping(update_mapping),
    m_remove_global_image_ids(remove_global_image_ids),
    m_on_finish(on_finish) {
}

template <typename I>
void UpdateRequest<I>::send() {
  dout(20) << dendl;

  update_image_map();
}

template <typename I>
void UpdateRequest<I>::update_image_map() {
  dout(20) << dendl;

  if (m_update_mapping.empty() && m_remove_global_image_ids.empty()) {
    finish(0);
    return;
  }

  uint32_t nr_updates = 0;
  librados::ObjectWriteOperation op;

  auto it1 = m_update_mapping.begin();
  while (it1 != m_update_mapping.end() && nr_updates++ < MAX_UPDATE) {
    librbd::cls_client::mirror_image_map_update(&op, it1->first, it1->second);
    it1 = m_update_mapping.erase(it1);
  }

  auto it2 = m_remove_global_image_ids.begin();
  while (it2 != m_remove_global_image_ids.end() && nr_updates++ < MAX_UPDATE) {
    librbd::cls_client::mirror_image_map_remove(&op, *it2);
    it2 = m_remove_global_image_ids.erase(it2);
  }

  librados::AioCompletion *aio_comp = create_rados_callback<
    UpdateRequest, &UpdateRequest::handle_update_image_map>(this);
  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void UpdateRequest<I>::handle_update_image_map(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update image map: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  update_image_map();
}

template <typename I>
void UpdateRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::UpdateRequest<librbd::ImageCtx>;
