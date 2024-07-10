// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GetUuidRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GetUuidRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_rados_callback;

template <typename I>
GetUuidRequest<I>::GetUuidRequest(
    librados::IoCtx& io_ctx, std::string* mirror_uuid, Context* on_finish)
  : m_mirror_uuid(mirror_uuid), m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())) {
  m_io_ctx.dup(io_ctx);
  m_io_ctx.set_namespace("");
}

template <typename I>
void GetUuidRequest<I>::send() {
  get_mirror_uuid();
}

template <typename I>
void GetUuidRequest<I>::get_mirror_uuid() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_uuid_get_start(&op);

  auto aio_comp = create_rados_callback<
    GetUuidRequest<I>, &GetUuidRequest<I>::handle_get_mirror_uuid>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GetUuidRequest<I>::handle_get_mirror_uuid(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r >= 0) {
    auto it = m_out_bl.cbegin();
    r = librbd::cls_client::mirror_uuid_get_finish(&it, m_mirror_uuid);
    if (r >= 0 && m_mirror_uuid->empty()) {
      r = -ENOENT;
    }
  }

  if (r < 0) {
    if (r == -ENOENT) {
      ldout(m_cct, 5) << "mirror uuid missing" << dendl;
    } else {
      lderr(m_cct) << "failed to retrieve mirror uuid: " << cpp_strerror(r)
                   << dendl;
    }
    *m_mirror_uuid = "";
  }

  finish(r);
}

template <typename I>
void GetUuidRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GetUuidRequest<librbd::ImageCtx>;
