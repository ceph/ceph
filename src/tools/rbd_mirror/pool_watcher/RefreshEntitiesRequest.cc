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

  finish(0);
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
