// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LogOperation.h"
#include "common/debug.h"
#include "common/perf_counters.h"

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

void DiscardLogOperation::init_op(
    uint64_t current_sync_gen, bool persist_on_flush,
    uint64_t last_op_sequence_num, Context *write_persist,
    Context *write_append) {
  log_entry->init(current_sync_gen, persist_on_flush, last_op_sequence_num);
  this->on_write_append = write_append;
  this->on_write_persist = write_persist;
}

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd
