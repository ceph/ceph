// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_READ_REQUEST_H
#define CEPH_LIBRBD_CACHE_RWL_READ_REQUEST_H

#include "include/Context.h"
#include "librbd/cache/rwl/Types.h"

namespace librbd {
namespace cache {
namespace rwl {

typedef std::vector<rwl::ImageExtentBuf> ImageExtentBufs;

class C_ReadRequest : public Context {
public:
  io::Extents miss_extents; // move back to caller
  ImageExtentBufs read_extents;
  bufferlist miss_bl;

  C_ReadRequest(CephContext *cct, utime_t arrived, PerfCounters *perfcounter, bufferlist *out_bl, Context *on_finish)
    : m_cct(cct), m_on_finish(on_finish), m_out_bl(out_bl),
      m_arrived_time(arrived), m_perfcounter(perfcounter) {}
  ~C_ReadRequest() {}

  void finish(int r) override;

  const char *get_name() const {
    return "C_ReadRequest";
  }

private:
  CephContext *m_cct;
  Context *m_on_finish;
  bufferlist *m_out_bl;
  utime_t m_arrived_time;
  PerfCounters *m_perfcounter;
};

} // namespace rwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_RWL_READ_REQUEST_H
