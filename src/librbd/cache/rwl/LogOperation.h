// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_LOG_OPERATION_H
#define CEPH_LIBRBD_CACHE_RWL_LOG_OPERATION_H

#include "include/utime.h"
#include "librbd/cache/rwl/LogEntry.h"
#include "librbd/cache/rwl/SyncPoint.h"

namespace librbd {
namespace cache {
namespace rwl {
struct WriteBufferAllocation;

class WriteLogOperationSet;

class WriteLogOperation;

class GenericWriteLogOperation;

class SyncPointLogOperation;

class GenericLogOperation;

using GenericLogOperationSharedPtr = std::shared_ptr<GenericLogOperation>;

using GenericLogOperationsVector = std::vector<GenericLogOperationSharedPtr>;

class GenericLogOperation {
protected:
  PerfCounters *m_perfcounter = nullptr;
public:
  utime_t dispatch_time;         // When op created
  utime_t buf_persist_time;      // When buffer persist begins
  utime_t buf_persist_comp_time; // When buffer persist completes
  utime_t log_append_time;       // When log append begins
  utime_t log_append_comp_time;  // When log append completes
  GenericLogOperation(const utime_t dispatch_time, PerfCounters *perfcounter);
  virtual ~GenericLogOperation() { };
  GenericLogOperation(const GenericLogOperation&) = delete;
  GenericLogOperation &operator=(const GenericLogOperation&) = delete;
  virtual std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const GenericLogOperation &op);
  virtual const std::shared_ptr<GenericLogEntry> get_log_entry() = 0;
  virtual void appending() = 0;
  virtual void complete(int r) = 0;
  virtual void mark_log_entry_completed() {};
  virtual bool reserved_allocated() const {
    return false;
  }
  virtual bool is_writing_op() const {
    return false;
  }
  virtual void copy_bl_to_pmem_buffer() {};
  virtual void flush_pmem_buf_to_cache(PMEMobjpool *log_pool) {};
};

class SyncPointLogOperation : public GenericLogOperation {
private:
  CephContext *m_cct;
  ceph::mutex &m_lock;
  std::vector<Context*> append_sync_point();
  void clear_earlier_sync_point();
  std::vector<Context*> swap_on_sync_point_persisted();
public:
  std::shared_ptr<SyncPoint> sync_point;
  SyncPointLogOperation(ceph::mutex &lock,
                        std::shared_ptr<SyncPoint> sync_point,
                        const utime_t dispatch_time,
                        PerfCounters *perfcounter,
                        CephContext *cct);
  ~SyncPointLogOperation() override;
  SyncPointLogOperation(const SyncPointLogOperation&) = delete;
  SyncPointLogOperation &operator=(const SyncPointLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const SyncPointLogOperation &op);
  const std::shared_ptr<GenericLogEntry> get_log_entry() override {
    return sync_point->log_entry;
  }
  void appending() override;
  void complete(int r) override;
};

class GenericWriteLogOperation : public GenericLogOperation {
protected:
  ceph::mutex m_lock;
  CephContext *m_cct;
public:
  std::shared_ptr<SyncPoint> sync_point;
  Context *on_write_append = nullptr;  /* Completion for things waiting on this
                                        * write's position in the log to be
                                        * guaranteed */
  Context *on_write_persist = nullptr; /* Completion for things waiting on this
                                        * write to persist */
  GenericWriteLogOperation(std::shared_ptr<SyncPoint> sync_point,
                           const utime_t dispatch_time,
                           PerfCounters *perfcounter,
                           CephContext *cct);
  ~GenericWriteLogOperation() override;
  GenericWriteLogOperation(const GenericWriteLogOperation&) = delete;
  GenericWriteLogOperation &operator=(const GenericWriteLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const GenericWriteLogOperation &op);
  void mark_log_entry_completed() override{
    sync_point->log_entry->writes_completed++;
  }
  bool reserved_allocated() const override {
    return true;
  }
  bool is_writing_op() const override {
    return true;
  }
  void appending() override;
  void complete(int r) override;
};

class WriteLogOperation : public GenericWriteLogOperation {
public:
  using GenericWriteLogOperation::m_lock;
  using GenericWriteLogOperation::sync_point;
  using GenericWriteLogOperation::on_write_append;
  using GenericWriteLogOperation::on_write_persist;
  std::shared_ptr<WriteLogEntry> log_entry;
  bufferlist bl;
  WriteBufferAllocation *buffer_alloc = nullptr;
  WriteLogOperation(WriteLogOperationSet &set, const uint64_t image_offset_bytes,
                    const uint64_t write_bytes, CephContext *cct);
  ~WriteLogOperation() override;
  WriteLogOperation(const WriteLogOperation&) = delete;
  WriteLogOperation &operator=(const WriteLogOperation&) = delete;
  void init(bool has_data, std::vector<WriteBufferAllocation>::iterator allocation, uint64_t current_sync_gen,
            uint64_t last_op_sequence_num, bufferlist &write_req_bl, uint64_t buffer_offset,
            bool persist_on_flush);
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const WriteLogOperation &op);
  const std::shared_ptr<GenericLogEntry> get_log_entry() override {
    return log_entry;
  }

  void complete(int r) override;
  void copy_bl_to_pmem_buffer() override;
  void flush_pmem_buf_to_cache(PMEMobjpool *log_pool) override;
};


class WriteLogOperationSet {
private:
  CephContext *m_cct;
  Context *m_on_finish;
public:
  bool persist_on_flush;
  BlockGuardCell *cell;
  C_Gather *extent_ops_appending;
  Context *on_ops_appending;
  C_Gather *extent_ops_persist;
  Context *on_ops_persist;
  GenericLogOperationsVector operations;
  utime_t dispatch_time; /* When set created */
  PerfCounters *perfcounter = nullptr;
  std::shared_ptr<SyncPoint> sync_point;
  WriteLogOperationSet(const utime_t dispatched, PerfCounters *perfcounter, std::shared_ptr<SyncPoint> sync_point,
                       const bool persist_on_flush, CephContext *cct, Context *on_finish);
  ~WriteLogOperationSet();
  WriteLogOperationSet(const WriteLogOperationSet&) = delete;
  WriteLogOperationSet &operator=(const WriteLogOperationSet&) = delete;
  friend std::ostream &operator<<(std::ostream &os,
                                  const WriteLogOperationSet &s);
};

class DiscardLogOperation : public GenericWriteLogOperation {
public:
  using GenericWriteLogOperation::m_lock;
  using GenericWriteLogOperation::sync_point;
  using GenericWriteLogOperation::on_write_append;
  using GenericWriteLogOperation::on_write_persist;
  std::shared_ptr<DiscardLogEntry> log_entry;
  DiscardLogOperation(std::shared_ptr<SyncPoint> sync_point,
                      const uint64_t image_offset_bytes,
                      const uint64_t write_bytes,
                      uint32_t discard_granularity_bytes,
                      const utime_t dispatch_time,
                      PerfCounters *perfcounter,
                      CephContext *cct);
  ~DiscardLogOperation() override;
  DiscardLogOperation(const DiscardLogOperation&) = delete;
  DiscardLogOperation &operator=(const DiscardLogOperation&) = delete;
  const std::shared_ptr<GenericLogEntry> get_log_entry() override {
    return log_entry;
  }
  bool reserved_allocated() const override {
    return false;
  }
  void init(uint64_t current_sync_gen, bool persist_on_flush,
            uint64_t last_op_sequence_num, Context *write_persist);
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const DiscardLogOperation &op);
};

class WriteSameLogOperation : public WriteLogOperation {
public:
  using GenericWriteLogOperation::m_lock;
  using GenericWriteLogOperation::sync_point;
  using GenericWriteLogOperation::on_write_append;
  using GenericWriteLogOperation::on_write_persist;
  using WriteLogOperation::log_entry;
  using WriteLogOperation::bl;
  using WriteLogOperation::buffer_alloc;
  WriteSameLogOperation(WriteLogOperationSet &set,
                        const uint64_t image_offset_bytes,
                        const uint64_t write_bytes,
                        const uint32_t data_len,
                        CephContext *cct);
  ~WriteSameLogOperation();
  WriteSameLogOperation(const WriteSameLogOperation&) = delete;
  WriteSameLogOperation &operator=(const WriteSameLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const;
  friend std::ostream &operator<<(std::ostream &os,
                                  const WriteSameLogOperation &op);
};

} // namespace rwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_RWL_LOG_OPERATION_H
