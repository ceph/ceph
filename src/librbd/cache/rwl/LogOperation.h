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

template <typename T>
class WriteLogOperationSet;

template <typename T>
class WriteLogOperation;

template <typename T>
class GeneralWriteLogOperation;

template <typename T>
class SyncPointLogOperation;

template <typename T>
class GenericLogOperation;

template <typename T>
using GenericLogOperationSharedPtr = std::shared_ptr<GenericLogOperation<T>>;

template <typename T>
using GenericLogOperationsVector = std::vector<GenericLogOperationSharedPtr<T>>;

template <typename T>
class GenericLogOperation {
public:
  T &rwl;
  utime_t dispatch_time;         // When op created
  utime_t buf_persist_time;      // When buffer persist begins
  utime_t buf_persist_comp_time; // When buffer persist completes
  utime_t log_append_time;       // When log append begins
  utime_t log_append_comp_time;  // When log append completes
  GenericLogOperation(T &rwl, const utime_t dispatch_time);
  virtual ~GenericLogOperation() { };
  GenericLogOperation(const GenericLogOperation&) = delete;
  GenericLogOperation &operator=(const GenericLogOperation&) = delete;
  virtual std::ostream &format(std::ostream &os) const;
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const GenericLogOperation<U> &op);
  virtual const std::shared_ptr<GenericLogEntry> get_log_entry() = 0;
  virtual const std::shared_ptr<SyncPointLogEntry> get_sync_point_log_entry() { return nullptr; }
  virtual const std::shared_ptr<GeneralWriteLogEntry> get_gen_write_log_entry() { return nullptr; }
  virtual const std::shared_ptr<WriteLogEntry> get_write_log_entry() { return nullptr; }
  virtual void appending() = 0;
  virtual void complete(int r) = 0;
  virtual bool is_write() { return false; }
  virtual bool is_sync_point() { return false; }
  virtual bool is_discard() { return false; }
  virtual bool is_writesame() { return false; }
  virtual bool is_writing_op() { return false; }
  virtual GeneralWriteLogOperation<T> *get_gen_write_op() { return nullptr; };
  virtual WriteLogOperation<T> *get_write_op() { return nullptr; };
};

template <typename T>
class SyncPointLogOperation : public GenericLogOperation<T> {
public:
  using GenericLogOperation<T>::rwl;
  std::shared_ptr<SyncPoint<T>> sync_point;
  SyncPointLogOperation(T &rwl,
                        std::shared_ptr<SyncPoint<T>> sync_point,
                        const utime_t dispatch_time);
  ~SyncPointLogOperation();
  SyncPointLogOperation(const SyncPointLogOperation&) = delete;
  SyncPointLogOperation &operator=(const SyncPointLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const;
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const SyncPointLogOperation<U> &op);
  const std::shared_ptr<GenericLogEntry> get_log_entry() { return get_sync_point_log_entry(); }
  const std::shared_ptr<SyncPointLogEntry> get_sync_point_log_entry() { return sync_point->log_entry; }
  bool is_sync_point() { return true; }
  void appending();
  void complete(int r);
};

template <typename T>
class GeneralWriteLogOperation : public GenericLogOperation<T> {
protected:
  ceph::mutex m_lock;
public:
  using GenericLogOperation<T>::rwl;
  std::shared_ptr<SyncPoint<T>> sync_point;
  Context *on_write_append = nullptr;  /* Completion for things waiting on this
                                        * write's position in the log to be
                                        * guaranteed */
  Context *on_write_persist = nullptr; /* Completion for things waiting on this
                                        * write to persist */
  GeneralWriteLogOperation(T &rwl,
                           std::shared_ptr<SyncPoint<T>> sync_point,
                           const utime_t dispatch_time);
  ~GeneralWriteLogOperation();
  GeneralWriteLogOperation(const GeneralWriteLogOperation&) = delete;
  GeneralWriteLogOperation &operator=(const GeneralWriteLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const;
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const GeneralWriteLogOperation<U> &op);
  GeneralWriteLogOperation<T> *get_gen_write_op() { return this; };
  bool is_writing_op() { return true; }
  void appending();
  void complete(int r);
};

template <typename T>
class WriteLogOperation : public GeneralWriteLogOperation<T> {
public:
  using GenericLogOperation<T>::rwl;
  using GeneralWriteLogOperation<T>::m_lock;
  using GeneralWriteLogOperation<T>::sync_point;
  using GeneralWriteLogOperation<T>::on_write_append;
  using GeneralWriteLogOperation<T>::on_write_persist;
  std::shared_ptr<WriteLogEntry> log_entry;
  bufferlist bl;
  WriteBufferAllocation *buffer_alloc = nullptr;
  WriteLogOperation(WriteLogOperationSet<T> &set, const uint64_t image_offset_bytes, const uint64_t write_bytes);
  ~WriteLogOperation();
  WriteLogOperation(const WriteLogOperation&) = delete;
  WriteLogOperation &operator=(const WriteLogOperation&) = delete;
  std::ostream &format(std::ostream &os) const;
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const WriteLogOperation<T> &op);
  const std::shared_ptr<GenericLogEntry> get_log_entry() { return get_write_log_entry(); }
  const std::shared_ptr<WriteLogEntry> get_write_log_entry() { return log_entry; }
  WriteLogOperation<T> *get_write_op() override { return this; }
  bool is_write() { return true; }
};


template <typename T>
class WriteLogOperationSet {
private:
  Context *m_on_finish;
public:
  T &rwl;
  bool persist_on_flush;
  BlockGuardCell *cell;
  C_Gather *extent_ops_appending;
  Context *on_ops_appending;
  C_Gather *extent_ops_persist;
  Context *on_ops_persist;
  GenericLogOperationsVector<T> operations;
  utime_t dispatch_time; /* When set created */
  std::shared_ptr<SyncPoint<T>> sync_point;
  WriteLogOperationSet(T &rwl, const utime_t dispatched, std::shared_ptr<SyncPoint<T>> sync_point,
                       const bool persist_on_flush, Context *on_finish);
  ~WriteLogOperationSet();
  WriteLogOperationSet(const WriteLogOperationSet&) = delete;
  WriteLogOperationSet &operator=(const WriteLogOperationSet&) = delete;
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const WriteLogOperationSet<U> &s);
};

} // namespace rwl 
} // namespace cache 
} // namespace librbd 

#endif // CEPH_LIBRBD_CACHE_RWL_LOG_OPERATION_H
