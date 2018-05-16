// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/CreateRequest.h"
#include "include/assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "osdc/Striper.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::CreateRequest: "

namespace librbd {
namespace object_map {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
CreateRequest<I>::CreateRequest(I *image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish) {
}

template <typename I>
void CreateRequest<I>::send() {
  CephContext *cct = m_image_ctx->cct;

  uint64_t max_size = m_image_ctx->size;

  {
    RWLock::WLocker snap_locker(m_image_ctx->snap_lock);
    m_snap_ids.push_back(CEPH_NOSNAP);
    for (auto it : m_image_ctx->snap_info) {
      max_size = std::max(max_size, it.second.size);
      m_snap_ids.push_back(it.first);
    }

    if (ObjectMap<>::is_compatible(m_image_ctx->layout, max_size)) {
      send_object_map_resize();
      return;
    }
  }

  lderr(cct) << "image size not compatible with object map" << dendl;
  m_on_finish->complete(-EINVAL);
}

template <typename I>
void CreateRequest<I>::send_object_map_resize() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  Context *ctx = create_context_callback<
    CreateRequest<I>, &CreateRequest<I>::handle_object_map_resize>(this);
  C_Gather *gather_ctx = new C_Gather(cct, ctx);

  for (auto snap_id : m_snap_ids) {
    librados::ObjectWriteOperation op;
    uint64_t snap_size = m_image_ctx->get_image_size(snap_id);

    cls_client::object_map_resize(&op, Striper::get_num_objects(
				    m_image_ctx->layout, snap_size),
				  OBJECT_NONEXISTENT);

    std::string oid(ObjectMap<>::object_map_name(m_image_ctx->id, snap_id));
    librados::AioCompletion *comp = create_rados_callback(gather_ctx->new_sub());
    int r = m_image_ctx->md_ctx.aio_operate(oid, comp, &op);
    assert(r == 0);
    comp->release();
  }
  gather_ctx->activate();
}

template <typename I>
Context *CreateRequest<I>::handle_object_map_resize(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "object map resize failed: " << cpp_strerror(*result)
	       << dendl;
  }
  return m_on_finish;
}

} // namespace object_map
} // namespace librbd

template class librbd::object_map::CreateRequest<librbd::ImageCtx>;
