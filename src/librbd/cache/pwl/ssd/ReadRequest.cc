// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ReadRequest.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/debug.h"
#include "common/perf_counters.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ssd::ReadRequest: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
namespace ssd {

void C_ReadRequest::finish(int r) {
  ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << dendl;
  int hits = 0;
  int misses = 0;
  int hit_bytes = 0;
  int miss_bytes = 0;
  if (r >= 0) {
      /*
       * At this point the miss read has completed. We'll iterate through
       * m_read_extents and produce *m_out_bl by assembling pieces of m_miss_bl
       * and the individual hit extent bufs in the read extents that represent
       * hits.
       */
    uint64_t miss_bl_offset = 0;
    for (auto extent : read_extents) {
      if (extent->m_bl.length()) {
        /* This was a hit */
        bufferlist data_bl;
        if (extent->writesame) {
          int data_len = extent->m_bl.length();
          int read_buffer_offset = extent->truncate_offset;
          if (extent->need_to_truncate && extent->truncate_offset >= data_len) {
            read_buffer_offset = (extent->truncate_offset) % data_len;
          }
          // build data and truncate
          bufferlist temp_bl;
          uint64_t total_left_bytes = read_buffer_offset + extent->second;
          while (total_left_bytes > 0) {
            temp_bl.append(extent->m_bl);
            total_left_bytes = total_left_bytes - data_len;
          }
          data_bl.substr_of(temp_bl, read_buffer_offset, extent->second);
          m_out_bl->claim_append(data_bl);
        } else if (extent->need_to_truncate) {
          assert(extent->m_bl.length() >= extent->truncate_offset + extent->second);
          data_bl.substr_of(extent->m_bl, extent->truncate_offset, extent->second);
          m_out_bl->claim_append(data_bl);
        } else {
          assert(extent->second == extent->m_bl.length());
          m_out_bl->claim_append(extent->m_bl);
        }
        ++hits;
        hit_bytes += extent->second;
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

} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd
