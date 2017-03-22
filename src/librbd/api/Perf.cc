// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Perf.h"
#include "common/dout.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::Perf: " << __func__ << ": "

namespace librbd {
namespace api {

template <typename I>
int Perf<I>::perf_reset(I *ictx)
{
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << ictx << dendl;

  return cls_client::image_perf_reset(&ictx->md_ctx, ictx->header_oid);
}

template <typename I>
int Perf<I>::perf_dump(I *ictx, const std::string &format, bufferlist *outbl)
{
  CephContext *cct = ictx->cct;
  ldout(cct, 20) << ictx << dendl;

  cls::rbd::PerfCounters perf;
  int r = cls_client::image_perf_get(&ictx->md_ctx, ictx->header_oid, &perf);
  if (r < 0) {
    return r;
  }

  Formatter *f = Formatter::create(format);
  if (f == nullptr) {
    return -EINVAL;
  }

  perf.dump(f);
  f->flush(*outbl);
  delete f;

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Perf<librbd::ImageCtx>;
