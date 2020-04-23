// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "LogOperation.h"
#include "librbd/cache/rwl/Types.h"

#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::LogOperation: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {

namespace cache {

namespace rwl {

GenericLogOperation::GenericLogOperation(const utime_t dispatch_time, PerfCounters *perfcounter)
  : m_perfcounter(perfcounter), dispatch_time(dispatch_time) {
}

std::ostream& GenericLogOperation::format(std::ostream &os) const {
  os << "dispatch_time=[" << dispatch_time << "], "
     << "buf_persist_time=[" << buf_persist_time << "], "
     << "buf_persist_comp_time=[" << buf_persist_comp_time << "], "
     << "log_append_time=[" << log_append_time << "], "
     << "log_append_comp_time=[" << log_append_comp_time << "], ";
  return os;
};

std::ostream &operator<<(std::ostream &os,
                         const GenericLogOperation &op) {
  return op.format(os);
}

SyncPointLogOperation::SyncPointLogOperation(ceph::mutex &lock,
                                             std::shared_ptr<SyncPoint> sync_point,
                                             const utime_t dispatch_time,
                                             PerfCounters *perfcounter,
                                             CephContext *cct)
  : GenericLogOperation(dispatch_time, perfcounter), m_cct(cct), m_lock(lock), sync_point(sync_point) {
}

SyncPointLogOperation::~SyncPointLogOperation() { }

std::ostream &SyncPointLogOperation::format(std::ostream &os) const {
  os << "(Sync Point) ";
  GenericLogOperation::format(os);
  os << ", "
     << "sync_point=[" << *sync_point << "]";
  return os;
};

std::ostream &operator<<(std::ostream &os,
                         const SyncPointLogOperation &op) {
  return op.format(os);
}

std::vector<Context*> SyncPointLogOperation::append_sync_point() {
  std::vector<Context*> appending_contexts;
  std::lock_guard locker(m_lock);
  if (!sync_point->appending) {
    sync_point->appending = true;
  }
  appending_contexts.swap(sync_point->on_sync_point_appending);
  return appending_contexts;
}

void SyncPointLogOperation::clear_earlier_sync_point() {
  std::lock_guard locker(m_lock);
  ceph_assert(sync_point->later_sync_point);
  ceph_assert(sync_point->later_sync_point->earlier_sync_point ==
              sync_point);
  sync_point->later_sync_point->earlier_sync_point = nullptr;
}

std::vector<Context*> SyncPointLogOperation::swap_on_sync_point_persisted() {
  std::lock_guard locker(m_lock);
  std::vector<Context*> persisted_contexts;
  persisted_contexts.swap(sync_point->on_sync_point_persisted);
  return persisted_contexts;
}

void SyncPointLogOperation::appending() {
  ceph_assert(sync_point);
  ldout(m_cct, 20) << "Sync point op=[" << *this
                   << "] appending" << dendl;
  auto appending_contexts = append_sync_point();
  for (auto &ctx : appending_contexts) {
    ctx->complete(0);
  }
}

void SyncPointLogOperation::complete(int result) {
  ceph_assert(sync_point);
  ldout(m_cct, 20) << "Sync point op =[" << *this
                   << "] completed" << dendl;
  clear_earlier_sync_point();

  /* Do append now in case completion occurred before the
   * normal append callback executed, and to handle
   * on_append work that was queued after the sync point
   * entered the appending state. */
  appending();
  auto persisted_contexts = swap_on_sync_point_persisted();
  for (auto &ctx : persisted_contexts) {
    ctx->complete(result);
  }
}

GenericWriteLogOperation::GenericWriteLogOperation(std::shared_ptr<SyncPoint> sync_point,
                                                   const utime_t dispatch_time,
                                                   PerfCounters *perfcounter,
                                                   CephContext *cct)
  : GenericLogOperation(dispatch_time, perfcounter),
  m_lock(ceph::make_mutex(util::unique_lock_name(
    "librbd::cache::rwl::GenericWriteLogOperation::m_lock", this))),
  m_cct(cct),
  sync_point(sync_point) {
}

GenericWriteLogOperation::~GenericWriteLogOperation() { }

std::ostream &GenericWriteLogOperation::format(std::ostream &os) const {
  GenericLogOperation::format(os);
  return os;
};

std::ostream &operator<<(std::ostream &os,
                         const GenericWriteLogOperation &op) {
  return op.format(os);
}

/* Called when the write log operation is appending and its log position is guaranteed */
void GenericWriteLogOperation::appending() {
  Context *on_append = nullptr;
  ldout(m_cct, 20) << __func__ << " " << this << dendl;
  {
    std::lock_guard locker(m_lock);
    on_append = on_write_append;
    on_write_append = nullptr;
  }
  if (on_append) {
    ldout(m_cct, 20) << __func__ << " " << this << " on_append=" << on_append << dendl;
    on_append->complete(0);
  }
}

/* Called when the write log operation is completed in all log replicas */
void GenericWriteLogOperation::complete(int result) {
  appending();
  Context *on_persist = nullptr;
  ldout(m_cct, 20) << __func__ << " " << this << dendl;
  {
    std::lock_guard locker(m_lock);
    on_persist = on_write_persist;
    on_write_persist = nullptr;
  }
  if (on_persist) {
    ldout(m_cct, 20) << __func__ << " " << this << " on_persist=" << on_persist << dendl;
    on_persist->complete(result);
  }
}

WriteLogOperation::WriteLogOperation(WriteLogOperationSet &set,
                                     uint64_t image_offset_bytes, uint64_t write_bytes,
                                     CephContext *cct)
  : GenericWriteLogOperation(set.sync_point, set.dispatch_time, set.perfcounter, cct),
    log_entry(std::make_shared<WriteLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes)) {
  on_write_append = set.extent_ops_appending->new_sub();
  on_write_persist = set.extent_ops_persist->new_sub();
  log_entry->sync_point_entry->writes++;
  log_entry->sync_point_entry->bytes += write_bytes;
}

WriteLogOperation::~WriteLogOperation() { }

void WriteLogOperation::init(bool has_data, std::vector<WriteBufferAllocation>::iterator allocation, uint64_t current_sync_gen,
                             uint64_t last_op_sequence_num, bufferlist &write_req_bl, uint64_t buffer_offset,
                             bool persist_on_flush) {
  log_entry->init(has_data, allocation, current_sync_gen, last_op_sequence_num, persist_on_flush);
  buffer_alloc = &(*allocation);
  bl.substr_of(write_req_bl, buffer_offset,
               log_entry->write_bytes());
}

std::ostream &WriteLogOperation::format(std::ostream &os) const {
  os << "(Write) ";
  GenericWriteLogOperation::format(os);
  os << ", ";
  if (log_entry) {
    os << "log_entry=[" << *log_entry << "], ";
  } else {
    os << "log_entry=nullptr, ";
  }
  os << "bl=[" << bl << "],"
     << "buffer_alloc=" << buffer_alloc;
  return os;
};

std::ostream &operator<<(std::ostream &os,
                         const WriteLogOperation &op) {
  return op.format(os);
}


void WriteLogOperation::complete(int result) {
  GenericWriteLogOperation::complete(result);
  m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_buf_t, buf_persist_time - dispatch_time);
  utime_t buf_lat = buf_persist_comp_time - buf_persist_time;
  m_perfcounter->tinc(l_librbd_rwl_log_op_buf_to_bufc_t, buf_lat);
  m_perfcounter->hinc(l_librbd_rwl_log_op_buf_to_bufc_t_hist, buf_lat.to_nsec(),
                      log_entry->ram_entry.write_bytes);
  m_perfcounter->tinc(l_librbd_rwl_log_op_buf_to_app_t, log_append_time - buf_persist_time);
}

void WriteLogOperation::copy_bl_to_pmem_buffer() {
  /* operation is a shared_ptr, so write_op is only good as long as operation is in scope */
  bufferlist::iterator i(&bl);
  m_perfcounter->inc(l_librbd_rwl_log_op_bytes, log_entry->write_bytes());
  ldout(m_cct, 20) << bl << dendl;
  i.copy((unsigned)log_entry->write_bytes(), (char*)log_entry->pmem_buffer);
}

void WriteLogOperation::flush_pmem_buf_to_cache(PMEMobjpool *log_pool) {
  buf_persist_time = ceph_clock_now();
  pmemobj_flush(log_pool, log_entry->pmem_buffer, log_entry->write_bytes());
}

WriteLogOperationSet::WriteLogOperationSet(utime_t dispatched, PerfCounters *perfcounter, std::shared_ptr<SyncPoint> sync_point,
                                           bool persist_on_flush, CephContext *cct, Context *on_finish)
  : m_cct(cct), m_on_finish(on_finish),
    persist_on_flush(persist_on_flush),
    dispatch_time(dispatched),
    perfcounter(perfcounter),
    sync_point(sync_point) {
  on_ops_appending = sync_point->prior_persisted_gather_new_sub();
  on_ops_persist = nullptr;
  extent_ops_persist =
    new C_Gather(m_cct,
                 new LambdaContext( [this](int r) {
                     ldout(this->m_cct,20) << __func__ << " " << this << " m_extent_ops_persist completed" << dendl;
                     if (on_ops_persist) {
                       on_ops_persist->complete(r);
                     }
                     m_on_finish->complete(r);
                   }));
  auto appending_persist_sub = extent_ops_persist->new_sub();
  extent_ops_appending =
    new C_Gather(m_cct,
                 new LambdaContext( [this, appending_persist_sub](int r) {
                     ldout(this->m_cct, 20) << __func__ << " " << this << " m_extent_ops_appending completed" << dendl;
                     on_ops_appending->complete(r);
                     appending_persist_sub->complete(r);
                   }));
}

WriteLogOperationSet::~WriteLogOperationSet() { }

std::ostream &operator<<(std::ostream &os,
                         const WriteLogOperationSet &s) {
  os << "cell=" << (void*)s.cell << ", "
     << "extent_ops_appending=[" << s.extent_ops_appending << ", "
     << "extent_ops_persist=[" << s.extent_ops_persist << "]";
  return os;
};

DiscardLogOperation::DiscardLogOperation(std::shared_ptr<SyncPoint> sync_point,
                                         const uint64_t image_offset_bytes,
                                         const uint64_t write_bytes,
                                         uint32_t discard_granularity_bytes,
                                         const utime_t dispatch_time,
                                         PerfCounters *perfcounter,
                                         CephContext *cct)
  : GenericWriteLogOperation(sync_point, dispatch_time, perfcounter, cct),
    log_entry(std::make_shared<DiscardLogEntry>(sync_point->log_entry,
                                                image_offset_bytes,
                                                write_bytes,
                                                discard_granularity_bytes)) {
  on_write_append = sync_point->prior_persisted_gather_new_sub();
  on_write_persist = nullptr;
  log_entry->sync_point_entry->writes++;
  log_entry->sync_point_entry->bytes += write_bytes;
}

DiscardLogOperation::~DiscardLogOperation() { }

void DiscardLogOperation::init(uint64_t current_sync_gen, bool persist_on_flush,
                               uint64_t last_op_sequence_num, Context *write_persist) {
  log_entry->init(current_sync_gen, persist_on_flush, last_op_sequence_num);
  this->on_write_persist = write_persist;
}

std::ostream &DiscardLogOperation::format(std::ostream &os) const {
  os << "(Discard) ";
  GenericWriteLogOperation::format(os);
  os << ", ";
  if (log_entry) {
    os << "log_entry=[" << *log_entry << "], ";
  } else {
    os << "log_entry=nullptr, ";
  }
  return os;
};

std::ostream &operator<<(std::ostream &os,
                         const DiscardLogOperation &op) {
  return op.format(os);
}

WriteSameLogOperation::WriteSameLogOperation(WriteLogOperationSet &set,
                                             uint64_t image_offset_bytes,
                                             uint64_t write_bytes,
                                             uint32_t data_len,
                                             CephContext *cct)
  : WriteLogOperation(set, image_offset_bytes, write_bytes, cct) {
  log_entry =
    std::make_shared<WriteSameLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes, data_len);
  ldout(m_cct, 20) << __func__ << " " << this << dendl;
}

WriteSameLogOperation::~WriteSameLogOperation() { }

std::ostream &WriteSameLogOperation::format(std::ostream &os) const {
  os << "(Write Same) ";
  WriteLogOperation::format(os);
  return os;
};

std::ostream &operator<<(std::ostream &os,
                         const WriteSameLogOperation &op) {
  return op.format(os);
}

} // namespace rwl
} // namespace cache
} // namespace librbd
