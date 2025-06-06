// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ReadRequest.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/debug.h"
#include "common/perf_counters.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::rwl::ReadRequest: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
namespace rwl {

void C_ReadRequest::finish(int r) {
  ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << dendl;
  int hits = 0;
  int misses = 0;
  int hit_bytes = 0;
  int miss_bytes = 0;
  if (r >= 0) {
    /*
     * At this point the miss read has completed. We'll iterate through
     * read_extents and produce *m_out_bl by assembling pieces of miss_bl
     * and the individual hit extent bufs in the read extents that represent
     * hits.
     */
    uint64_t miss_bl_offset = 0;
    for (auto extent : read_extents) {
      if (extent->m_bl.length()) {
        /* This was a hit */
        ceph_assert(extent->second == extent->m_bl.length());
        ++hits;
        hit_bytes += extent->second;
        m_out_bl->claim_append(extent->m_bl);
      } else {
        /* This was a miss. */
        ++misses;
        miss_bytes += extent->second;
        bufferlist miss_extent_bl;
        miss_extent_bl.substr_of(miss_bl, miss_bl_offset, extent->second);
        /* Add this read miss bufferlist to the output bufferlist */
        m_out_bl->claim_append(miss_extent_bl);
        /* Consume these bytes in the read miss bufferlist */
        miss_bl_offset += extent->second;
      }
    }
  }
  ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << " bl=" << *m_out_bl << dendl;
  utime_t now = ceph_clock_now();
  ceph_assert((int)m_out_bl->length() == hit_bytes + miss_bytes);
  m_on_finish->complete(r);
  m_perfcounter->inc(l_librbd_pwl_rd_bytes, hit_bytes + miss_bytes);
  m_perfcounter->inc(l_librbd_pwl_rd_hit_bytes, hit_bytes);
  m_perfcounter->tinc(l_librbd_pwl_rd_latency, now - m_arrived_time);
  if (!misses) {
    m_perfcounter->inc(l_librbd_pwl_rd_hit_req, 1);
    m_perfcounter->tinc(l_librbd_pwl_rd_hit_latency, now - m_arrived_time);
  } else {
    if (hits) {
      m_perfcounter->inc(l_librbd_pwl_rd_part_hit_req, 1);
    }
  }
}

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd
