// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LogOperation.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::rwl::LogOperation: " \
                           << this << " " <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {
namespace rwl {

void WriteLogOperation::copy_bl_to_cache_buffer(
    std::vector<WriteBufferAllocation>::iterator allocation) {
  /* operation is a shared_ptr, so write_op is only good as long as operation is
   * in scope */
  bufferlist::iterator i(&bl);
  m_perfcounter->inc(l_librbd_pwl_log_op_bytes, log_entry->write_bytes());
  ldout(m_cct, 20) << bl << dendl;
  log_entry->init_cache_buffer(allocation);
  i.copy((unsigned)log_entry->write_bytes(), (char*)log_entry->cache_buffer);
}

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd
