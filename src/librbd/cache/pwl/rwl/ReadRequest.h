// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_RWL_READ_REQUEST_H
#define CEPH_LIBRBD_CACHE_PWL_RWL_READ_REQUEST_H

#include "librbd/cache/pwl/ReadRequest.h"

namespace librbd {
namespace cache {
namespace pwl {
namespace rwl {

typedef std::vector<pwl::ImageExtentBuf> ImageExtentBufs;

class C_ReadRequest : public pwl::C_ReadRequest {
protected:
  using pwl::C_ReadRequest::m_cct;
  using pwl::C_ReadRequest::m_on_finish;
  using pwl::C_ReadRequest::m_out_bl;
  using pwl::C_ReadRequest::m_arrived_time;
  using pwl::C_ReadRequest::m_perfcounter;
public:
  C_ReadRequest(CephContext *cct, utime_t arrived, PerfCounters *perfcounter, bufferlist *out_bl, Context *on_finish)
    : pwl::C_ReadRequest(cct, arrived, perfcounter, out_bl, on_finish) {}
  void finish(int r) override;
};

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_PWL_RWL_READ_REQUEST_H
